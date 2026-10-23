"""Asynchronous Telegram Bot alerting subsystem with exponential backoff retry queue for Aegis Quant (v0.2.0-rc2)."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import http.server
import json
import logging
import socketserver
import sqlite3
import threading
import urllib.request
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

from engine.database import DEFAULT_DB_PATH, DatabaseManager

logger = logging.getLogger("aegis.alerts")

HttpPostHandler = Callable[[str, dict[str, Any]], Awaitable[int]]


class AlertRetryQueue:
    """Asynchronous worker for reliable alert delivery with exponential backoff."""

    def __init__(
        self,
        db_manager: DatabaseManager | None = None,
        http_post: HttpPostHandler | None = None,
        telegram_api_base: str = "https://api.telegram.org",
    ) -> None:
        self.db = db_manager or DatabaseManager()
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._worker_task: asyncio.Task[Any] | None = None
        self._http_post = http_post
        self._telegram_api_base = telegram_api_base.rstrip("/")
        self.delivered_count: int = 0
        self.permanent_failure_count: int = 0

    def start(self) -> None:
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._process_queue())

    async def stop(self) -> None:
        if self._worker_task is not None and not self._worker_task.done():
            self._worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._worker_task
        self._worker_task = None

    async def enqueue_alert(self, title: str, message: str, level: str = "INFO") -> None:
        await self.queue.put({
            "title": title,
            "message": message,
            "level": level,
            "attempts": 0,
            "timestamp": int(datetime.now(timezone.utc).timestamp()),
        })

    async def _post_json(self, url: str, payload: dict[str, Any]) -> int:
        if self._http_post is not None:
            return int(await self._http_post(url, payload))

        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "AegisQuant-TelegramAlertEngine/0.2.0-rc2",
            },
            method="POST",
        )

        def _send() -> int:
            if url.startswith(("http://127.0.0.1", "http://localhost")):
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                with opener.open(req, timeout=5.0) as resp:
                    return int(getattr(resp, "status", 200))
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                return int(getattr(resp, "status", 200))

        return await asyncio.to_thread(_send)

    async def _dispatch_http(self, item: dict[str, Any]) -> bool:
        """Dispatch a queued alert item to Telegram or Discord webhook, returning True on 2xx."""
        cfg = self.db.get_decrypted_config()
        token = str(cfg.get("telegram_bot_token") or "").strip()
        chat_id = str(cfg.get("telegram_chat_id") or "").strip()
        webhook_url = str(cfg.get("discord_webhook_url") or "").strip()

        norm_level = str(item.get("level") or "INFO").strip().upper()
        title = str(item.get("title") or "")
        message = str(item.get("message") or "")
        formatted_msg = f"[{norm_level}] 🛡️ AEGIS QUANT: {title}\n{message}"

        dispatched = False
        if token and chat_id:
            tg_url = f"{self._telegram_api_base}/bot{token}/sendMessage"
            tg_payload: dict[str, Any] = {"chat_id": chat_id, "text": formatted_msg}
            try:
                status_code = await self._post_json(tg_url, tg_payload)
                if 200 <= status_code < 300:
                    dispatched = True
            except (OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
                logger.debug("Retry queue Telegram dispatch error: %s", exc)

        if webhook_url:
            dc_payload: dict[str, Any] = {"content": formatted_msg}
            try:
                status_code = await self._post_json(webhook_url, dc_payload)
                if 200 <= status_code < 300:
                    dispatched = True
            except (OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
                logger.debug("Retry queue Discord dispatch error: %s", exc)

        return dispatched

    async def _process_queue(self) -> None:
        while True:
            item = await self.queue.get()
            cfg = self.db.load_config(decrypt=False)
            max_retries = int(cfg.get("alert_max_retries", 5))
            base_delay = float(cfg.get("alert_backoff_base_sec", 2.0))

            try:
                success = await self._dispatch_http(item)
            except (OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
                logger.debug("Exception in _dispatch_http: %s", exc)
                success = False

            if success:
                self.delivered_count += 1
            elif item["attempts"] < max_retries:
                item["attempts"] += 1
                delay = base_delay * (2 ** (item["attempts"] - 1))
                logger.warning(
                    "Alert delivery failed ('%s'). Retrying in %.1fs (Attempt %d/%d)...",
                    item["title"], delay, item["attempts"], max_retries
                )
                await asyncio.sleep(delay)
                await self.queue.put(item)
            else:
                self.permanent_failure_count += 1
                logger.error("PERMANENT ALERT FAILURE: Exhausted %d retries for '%s'.", max_retries, item["title"])

            self.queue.task_done()


class AlertEngine:
    """
    Asynchronous alerting engine dispatching real-time notifications via Telegram Bot API
    (`telegram_bot_token` + `telegram_chat_id`) on:
    1. CRITICAL system log events or unhandled exceptions
    2. HMM Market Regime Flips (CHOP -> BULL / BEAR)
    3. On-chain position reconciliation mismatches
    4. Daily portfolio drawdown exceeding threshold (default 3.0%)
    """

    def __init__(
        self,
        db_manager: DatabaseManager | None = None,
        http_post: HttpPostHandler | None = None,
        telegram_api_base: str = "https://api.telegram.org",
    ) -> None:
        self.db = db_manager or DatabaseManager()
        self._http_post = http_post
        self._telegram_api_base = telegram_api_base.rstrip("/")
        self.retry_queue = AlertRetryQueue(
            self.db,
            http_post=http_post,
            telegram_api_base=self._telegram_api_base,
        )
        self._last_regime: str | None = None
        self._daily_drawdown_alerted: bool = False

    async def _post_json(self, url: str, payload: dict[str, Any]) -> int:
        """Dispatch an asynchronous HTTP POST JSON request with a 5-second timeout."""
        if self._http_post is not None:
            return int(await self._http_post(url, payload))

        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "AegisQuant-TelegramAlertEngine/0.2.0-rc2",
            },
            method="POST",
        )

        def _send() -> int:
            if url.startswith(("http://127.0.0.1", "http://localhost")):
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                with opener.open(req, timeout=5.0) as resp:
                    return int(getattr(resp, "status", 200))
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                return int(getattr(resp, "status", 200))

        return await asyncio.to_thread(_send)

    async def send_alert(
        self,
        title: str,
        message: str,
        level: str = "INFO",
    ) -> dict[str, Any]:
        """Format and dispatch an asynchronous alert to configured Telegram Bot API (and optional webhook) channels."""
        cfg = self.db.get_decrypted_config()
        token = str(cfg.get("telegram_bot_token") or "").strip()
        chat_id = str(cfg.get("telegram_chat_id") or "").strip()
        webhook_url = str(cfg.get("discord_webhook_url") or "").strip()

        norm_level = str(level or "INFO").strip().upper()
        formatted_msg = f"[{norm_level}] 🛡️ AEGIS QUANT: {title}\n{message}"

        telegram_dispatched = False
        discord_dispatched = False
        errors: list[str] = []

        if token and chat_id:
            tg_url = f"{self._telegram_api_base}/bot{token}/sendMessage"
            tg_payload: dict[str, Any] = {"chat_id": chat_id, "text": formatted_msg}
            try:
                status_code = await self._post_json(tg_url, tg_payload)
                telegram_dispatched = 200 <= status_code < 300
                if not telegram_dispatched:
                    errors.append(f"telegram: HTTP {status_code}")
            except (OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
                logger.warning("Telegram alert delivery failed: %s", exc)
                errors.append(f"telegram: {exc}")

        if webhook_url:
            dc_payload: dict[str, Any] = {"content": formatted_msg}
            try:
                status_code = await self._post_json(webhook_url, dc_payload)
                discord_dispatched = 200 <= status_code < 300
                if not discord_dispatched:
                    errors.append(f"discord: HTTP {status_code}")
            except (OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
                logger.warning("Webhook alert delivery failed: %s", exc)
                errors.append(f"discord: {exc}")

        if errors and not (telegram_dispatched or discord_dispatched):
            self.retry_queue._telegram_api_base = self._telegram_api_base
            self.retry_queue.start()
            await self.retry_queue.enqueue_alert(
                title=title, message=message, level=norm_level
            )

        report: dict[str, Any] = {
            "status": (
                "DELIVERED"
                if (telegram_dispatched or discord_dispatched)
                else ("QUEUED_LOCAL" if not errors else "PARTIAL_OR_FAILED")
            ),
            "channel": "TELEGRAM",
            "level": norm_level,
            "title": title,
            "message": message,
            "formatted_message": formatted_msg,
            "telegram_sent": telegram_dispatched,
            "discord_sent": discord_dispatched,
            "errors": errors,
        }
        try:
            self.db.log_telemetry(
                norm_level,
                "engine.alerts",
                formatted_msg,
                {
                    "channel": "TELEGRAM",
                    "title": title,
                    "telegram_sent": telegram_dispatched,
                    "discord_sent": discord_dispatched,
                },
            )
        except (sqlite3.Error, OSError, ValueError, TypeError, RuntimeError) as exc:
            logger.debug("Telemetry logging skipped during send_alert: %s", exc)
        return report

    async def notify_critical_event(
        self,
        title: str,
        message: str,
        exc: BaseException | None = None,
    ) -> dict[str, Any]:
        """Trigger 1: Dispatch a CRITICAL alert for system log events or unhandled exceptions."""
        detail = f"{message} | Exception: {exc!r}" if exc is not None else message
        return await self.send_alert(title=title, message=detail, level="CRITICAL")

    async def check_and_alert_regime_flip(
        self,
        previous_regime: str | None,
        new_regime: str,
        prob_bull: float = 0.0,
        prob_bear: float = 0.0,
        prob_chop: float = 0.0,
    ) -> dict[str, Any] | None:
        """Trigger 2: Detect HMM Market Regime flips (e.g., CHOP -> BULL / BEAR) and dispatch alert."""
        curr = str(new_regime or "CHOP").strip().upper()
        prev = (
            str(previous_regime).strip().upper()
            if previous_regime is not None
            else self._last_regime
        )
        if prev is None:
            try:
                with self.db.connection() as conn:
                    row = conn.execute(
                        "SELECT hmm_regime FROM portfolio_snapshots ORDER BY snapshot_id DESC LIMIT 1;"
                    ).fetchone()
                if row is not None and row["hmm_regime"]:
                    prev = str(row["hmm_regime"]).strip().upper()
            except (sqlite3.Error, OSError, ValueError, TypeError, KeyError):
                prev = None

        self._last_regime = curr

        if prev is None or prev == curr:
            return None

        msg = (
            f"HMM Regime transitioned {prev} -> {curr} "
            f"(Bull={prob_bull:.2f}, Bear={prob_bear:.2f}, Chop={prob_chop:.2f})"
        )
        return await self.send_alert(
            title=f"HMM Regime Flip: {prev} -> {curr}",
            message=msg,
            level="INFO" if curr == "CHOP" else "WARNING",
        )

    async def notify_reconciliation_mismatch(
        self,
        symbol: str,
        local_side: str,
        local_qty: float,
        on_chain_qty: float,
        action_taken: str = "EXCHANGE_RECONCILE",
    ) -> dict[str, Any]:
        """Trigger 3: Dispatch CRITICAL alert on on-chain position reconciliation mismatch."""
        msg = (
            f"Symbol {symbol} ({local_side}) local_qty={local_qty:.6f} vs "
            f"on_chain_qty={on_chain_qty:.6f}. Action={action_taken}."
        )
        return await self.send_alert(
            title="Position Drift Reconciliation",
            message=msg,
            level="CRITICAL",
        )

    async def check_and_alert_daily_drawdown(
        self,
        current_equity_usdc: float,
        reference_equity_usdc: float | None = None,
        threshold_pct: float | None = None,
    ) -> dict[str, Any] | None:
        """Trigger 4: Alert when daily portfolio drawdown exceeds configured threshold (default 3.0%)."""
        cfg = self.db.get_decrypted_config()
        limit_pct = (
            float(threshold_pct)
            if threshold_pct is not None
            else float(cfg.get("max_daily_drawdown_alert_pct", 0.03))
        )
        if reference_equity_usdc is not None:
            ref_eq = float(reference_equity_usdc)
        else:
            ref_eq = float(cfg.get("initial_equity_usdc", 10000.0))
            try:
                with self.db.connection() as conn:
                    max_ts_row = conn.execute(
                        "SELECT MAX(timestamp_ms) AS max_ts FROM portfolio_snapshots;"
                    ).fetchone()
                    if max_ts_row is not None and max_ts_row["max_ts"] is not None:
                        cutoff_ms = int(max_ts_row["max_ts"]) - 86_400_000
                        peak_row = conn.execute(
                            "SELECT MAX(total_equity_usdc) AS peak_eq FROM portfolio_snapshots WHERE timestamp_ms >= ?;",
                            (cutoff_ms,),
                        ).fetchone()
                        if peak_row is not None and peak_row["peak_eq"] is not None:
                            ref_eq = max(ref_eq, float(peak_row["peak_eq"]))
            except (sqlite3.Error, OSError, ValueError, TypeError, KeyError) as exc:
                logger.debug("Drawdown peak lookup fallback: %s", exc)

        if ref_eq <= 0.0:
            return None

        drawdown_pct = max(0.0, (ref_eq - float(current_equity_usdc)) / ref_eq)
        if drawdown_pct >= limit_pct:
            self._daily_drawdown_alerted = True
            msg = (
                f"Portfolio drawdown {drawdown_pct * 100.0:.2f}% exceeds threshold "
                f"{limit_pct * 100.0:.2f}% (Equity=${current_equity_usdc:.2f} vs Ref=${ref_eq:.2f})"
            )
            return await self.send_alert(
                title="Daily Drawdown Threshold Breached",
                message=msg,
                level="CRITICAL",
            )

        self._daily_drawdown_alerted = False
        return None

    async def run_test_alert(self, message: str, level: str = "INFO") -> dict[str, Any]:
        """
        Execute an end-to-end Telegram Bot alert delivery test.
        If no live Telegram credentials are configured in system_config, binds an ephemeral local
        HTTP Telegram Bot API receiver on 127.0.0.1 to verify full async HTTP POST payload delivery.
        """
        cfg = self.db.get_decrypted_config()
        has_live_telegram = bool(
            str(cfg.get("telegram_bot_token") or "").strip()
            and str(cfg.get("telegram_chat_id") or "").strip()
        )
        if has_live_telegram:
            res = await self.send_alert(
                title="Telegram Test Notification",
                message=message,
                level=level,
            )
            res["webhook_payload_verified"] = bool(res.get("telegram_sent"))
            return res

        received_payloads: list[dict[str, Any]] = []

        class _TelegramBotHandler(http.server.BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(length).decode("utf-8")
                try:
                    received_payloads.append(json.loads(raw_body))
                except (ValueError, TypeError):
                    received_payloads.append({"raw": raw_body})
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"ok": true}')

            def log_message(self, _format: str, *_args: Any) -> None:
                return

        class _ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
            allow_reuse_address = True

        prior_token_raw = self.db.get_raw_config_value("telegram_bot_token") or ""
        prior_chat_id = self.db.get_raw_config_value("telegram_chat_id") or ""
        prior_base = self._telegram_api_base
        with _ThreadedTCPServer(("127.0.0.1", 0), _TelegramBotHandler) as httpd:
            port = httpd.server_address[1]
            server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
            server_thread.start()
            try:
                self._telegram_api_base = f"http://127.0.0.1:{port}"
                self.db.set_config_value("telegram_bot_token", "000000000:AAH_loopback_test_token")
                self.db.set_config_value("telegram_chat_id", "-1001234567890")
                report = await self.send_alert(
                    title="Telegram Test Notification",
                    message=message,
                    level=level,
                )
                report["webhook_payload_verified"] = bool(
                    received_payloads
                    and received_payloads[-1].get("chat_id") == "-1001234567890"
                    and message in str(received_payloads[-1].get("text", ""))
                )
                return report
            finally:
                self._telegram_api_base = prior_base
                with self.db.connection() as conn:
                    conn.execute(
                        "UPDATE system_config SET config_value = ? WHERE config_key = 'telegram_bot_token';",
                        (prior_token_raw,),
                    )
                    conn.execute(
                        "UPDATE system_config SET config_value = ? WHERE config_key = 'telegram_chat_id';",
                        (prior_chat_id,),
                    )
                httpd.shutdown()
                server_thread.join(timeout=2.0)


def main() -> None:
    """CLI entrypoint for Aegis Quant Telegram Alerting Subsystem."""
    parser = argparse.ArgumentParser(
        description="Aegis Quant v0.2.0-rc1 Asynchronous Telegram Alerting Engine"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help="Path to SQLite database file (default: data/aegis_quant.db)",
    )
    parser.add_argument(
        "--test-alert",
        type=str,
        default=None,
        help="Send a test alert notification across configured/loopback Telegram Bot channel",
    )
    parser.add_argument(
        "--level",
        type=str,
        default="INFO",
        help="Alert severity level (INFO, WARNING, CRITICAL)",
    )
    args = parser.parse_args()

    db = DatabaseManager(db_path=args.db_path)
    engine = AlertEngine(db)
    alert_msg = args.test_alert or "v0.2.0-rc1 Deployment Ready"
    result = asyncio.run(engine.run_test_alert(alert_msg, level=args.level))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
