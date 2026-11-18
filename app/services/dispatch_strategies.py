from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from email.message import EmailMessage
import hashlib
import json
import socket
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
import urllib.error
import urllib.request
import uuid

from app.core.config import settings
from app.core.security import generate_webhook_signature
from app.schemas.event_schemas import ChannelType


@dataclass(frozen=True)
class DeliveryResult:
    success: bool
    status_code: Optional[int]
    latency_ms: float
    error_category: Optional[str] = None
    response_excerpt: Optional[str] = None
    retry_after_header: Optional[float] = None
    is_transient_failure: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


HTTPTransportCallable = Callable[
    [str, bytes, Dict[str, str], float],
    Tuple[int, str, Dict[str, str]],
]


def default_urllib_transport(
    url: str,
    body_bytes: bytes,
    headers: Dict[str, str],
    timeout_sec: float,
) -> Tuple[int, str, Dict[str, str]]:
    req = urllib.request.Request(url=url, data=body_bytes, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            status = resp.getcode()
            resp_body = resp.read().decode("utf-8", errors="replace")
            resp_headers = {k.lower(): v for k, v in resp.headers.items()}
            return status, resp_body, resp_headers
    except urllib.error.HTTPError as http_err:
        err_body = http_err.read().decode("utf-8", errors="replace") if http_err.fp else str(http_err)
        err_headers = {k.lower(): v for k, v in http_err.headers.items()} if http_err.headers else {}
        return http_err.code, err_body, err_headers


class DeliveryStrategy(ABC):
    @property
    @abstractmethod
    def channel_type(self) -> ChannelType:
        raise NotImplementedError

    @abstractmethod
    def dispatch(
        self,
        event_row: Dict[str, Any],
        timeout_sec: float = settings.WORKER_HTTP_TIMEOUT_SEC,
    ) -> DeliveryResult:
        raise NotImplementedError


class WebhookHTTPDispatcher(DeliveryStrategy):
    def __init__(self, transport: Optional[HTTPTransportCallable] = None) -> None:
        self._transport = transport or default_urllib_transport

    @property
    def channel_type(self) -> ChannelType:
        return ChannelType.WEBHOOK

    def dispatch(
        self,
        event_row: Dict[str, Any],
        timeout_sec: float = settings.WORKER_HTTP_TIMEOUT_SEC,
    ) -> DeliveryResult:
        start_ns = time.perf_counter_ns()
        payload_str: str = event_row["payload_json"]
        payload_bytes = payload_str.encode("utf-8")
        secret: str = event_row.get("signing_secret") or "whsec_relaycore_default_secret_key_2026"

        sig_header, sig_ts = generate_webhook_signature(payload_bytes, secret)
        custom_headers = json.loads(event_row.get("custom_headers_json") or "{}")

        headers: Dict[str, str] = {
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": "RelayCore-Webhook-Engine/1.0",
            "X-RelayCore-Event-ID": str(event_row["event_id"]),
            "X-RelayCore-Tenant-ID": str(event_row["tenant_id"]),
            "X-RelayCore-Event-Type": str(event_row["event_type"]),
            "X-RelayCore-Idempotency-Key": str(event_row["idempotency_key"]),
            "X-RelayCore-Signature": sig_header,
            "X-RelayCore-Timestamp": str(sig_ts),
            "X-RelayCore-Delivery-Attempt": str(int(event_row.get("retry_count", 0)) + 1),
        }
        for k, v in custom_headers.items():
            headers[str(k)] = str(v)

        try:
            status_code, resp_body, resp_headers = self._transport(
                event_row["destination"], payload_bytes, headers, timeout_sec
            )
            latency_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0

            retry_after: Optional[float] = None
            if "retry-after" in resp_headers:
                try:
                    retry_after = float(resp_headers["retry-after"])
                except ValueError:
                    retry_after = None

            if 200 <= status_code < 300:
                return DeliveryResult(
                    success=True,
                    status_code=status_code,
                    latency_ms=latency_ms,
                    response_excerpt=resp_body[:300],
                    is_transient_failure=False,
                    metadata={"signature_header": sig_header},
                )

            if status_code == 429:
                return DeliveryResult(
                    success=False,
                    status_code=429,
                    latency_ms=latency_ms,
                    error_category="UPSTREAM_RATE_LIMITED_429",
                    response_excerpt=resp_body[:300],
                    retry_after_header=retry_after,
                    is_transient_failure=True,
                )

            if status_code >= 500 or status_code == 408:
                return DeliveryResult(
                    success=False,
                    status_code=status_code,
                    latency_ms=latency_ms,
                    error_category="UPSTREAM_SERVER_ERROR_5XX",
                    response_excerpt=resp_body[:300],
                    retry_after_header=retry_after,
                    is_transient_failure=True,
                )

            return DeliveryResult(
                success=False,
                status_code=status_code,
                latency_ms=latency_ms,
                error_category="CLIENT_FATAL_4XX",
                response_excerpt=resp_body[:300],
                is_transient_failure=False,
            )

        except (socket.timeout, TimeoutError) as exc:
            latency_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            return DeliveryResult(
                success=False,
                status_code=504,
                latency_ms=latency_ms,
                error_category="NETWORK_SOCKET_TIMEOUT",
                response_excerpt=f"Socket timed out after {timeout_sec}s: {exc}",
                is_transient_failure=True,
            )
        except Exception as exc:
            latency_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            return DeliveryResult(
                success=False,
                status_code=502,
                latency_ms=latency_ms,
                error_category="NETWORK_CONNECTION_ERROR",
                response_excerpt=str(exc)[:300],
                is_transient_failure=True,
            )


class SlackBlockKitDispatcher(DeliveryStrategy):
    def __init__(self, transport: Optional[HTTPTransportCallable] = None) -> None:
        self._transport = transport or default_urllib_transport

    @property
    def channel_type(self) -> ChannelType:
        return ChannelType.SLACK

    def build_slack_block_kit_payload(self, event_row: Dict[str, Any]) -> Dict[str, Any]:
        raw_payload = json.loads(event_row["payload_json"])
        pretty_json = json.dumps(raw_payload, indent=2, sort_keys=True)
        return {
            "text": f"[RelayCore] Event {event_row['event_type']} for tenant {event_row['tenant_id']}",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"RelayCore Alert: {event_row['event_type']}",
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Tenant ID:*\n`{event_row['tenant_id']}`"},
                        {"type": "mrkdwn", "text": f"*Event ID:*\n`{event_row['event_id']}`"},
                        {"type": "mrkdwn", "text": f"*Attempt:*\n`#{int(event_row.get('retry_count', 0)) + 1}`"},
                        {"type": "mrkdwn", "text": f"*Idempotency Key:*\n`{event_row['idempotency_key']}`"},
                    ],
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Payload:*\n```{pretty_json}```",
                    },
                },
            ],
        }

    def dispatch(
        self,
        event_row: Dict[str, Any],
        timeout_sec: float = settings.WORKER_HTTP_TIMEOUT_SEC,
    ) -> DeliveryResult:
        start_ns = time.perf_counter_ns()
        slack_body = self.build_slack_block_kit_payload(event_row)
        body_bytes = json.dumps(slack_body).encode("utf-8")
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": "RelayCore-Slack-Dispatcher/1.0",
            "X-RelayCore-Event-ID": str(event_row["event_id"]),
            "X-RelayCore-Tenant-ID": str(event_row["tenant_id"]),
            "X-RelayCore-Idempotency-Key": str(event_row["idempotency_key"]),
            "X-RelayCore-Delivery-Attempt": str(int(event_row.get("retry_count", 0)) + 1),
        }
        try:
            status_code, resp_body, resp_headers = self._transport(
                event_row["destination"], body_bytes, headers, timeout_sec
            )
            latency_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            retry_after = float(resp_headers["retry-after"]) if "retry-after" in resp_headers else None

            if 200 <= status_code < 300:
                return DeliveryResult(
                    success=True,
                    status_code=status_code,
                    latency_ms=latency_ms,
                    response_excerpt=resp_body[:200] or "ok",
                    is_transient_failure=False,
                    metadata={"slack_blocks_count": len(slack_body["blocks"])},
                )
            is_transient = status_code in (408, 429) or status_code >= 500
            return DeliveryResult(
                success=False,
                status_code=status_code,
                latency_ms=latency_ms,
                error_category="SLACK_WEBHOOK_ERROR",
                response_excerpt=resp_body[:300],
                retry_after_header=retry_after,
                is_transient_failure=is_transient,
            )
        except Exception as exc:
            latency_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            return DeliveryResult(
                success=False,
                status_code=503,
                latency_ms=latency_ms,
                error_category="SLACK_NETWORK_FAILURE",
                response_excerpt=str(exc)[:300],
                is_transient_failure=True,
            )


class MockSMTPProtocolDispatcher(DeliveryStrategy):
    def __init__(self, sender_address: str = "notifications@relaycore.internal") -> None:
        self._sender = sender_address
        self._spooled_messages: List[Dict[str, Any]] = []

    @property
    def channel_type(self) -> ChannelType:
        return ChannelType.EMAIL

    @property
    def spooled_messages(self) -> List[Dict[str, Any]]:
        return list(self._spooled_messages)

    def build_mime_message(self, event_row: Dict[str, Any]) -> Tuple[EmailMessage, str]:
        recipient = event_row["destination"]
        payload = json.loads(event_row["payload_json"])
        subject = payload.get("subject") or f"[RelayCore] {event_row['event_type']} Notification"
        message_id = f"<{event_row['event_id']}.{int(time.time())}@relaycore.internal>"

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self._sender
        msg["To"] = recipient
        msg["Message-ID"] = message_id
        msg["X-RelayCore-Tenant"] = event_row["tenant_id"]
        msg["X-RelayCore-Event-ID"] = event_row["event_id"]
        msg["X-RelayCore-Idempotency-Key"] = event_row["idempotency_key"]

        body_digest = hashlib.sha256(event_row["payload_json"].encode("utf-8")).hexdigest()[:32]
        msg["DKIM-Signature"] = (
            f"v=1; a=rsa-sha256; d=relaycore.internal; s=selector1; bh={body_digest};"
        )

        plain_text = (
            f"Event Type: {event_row['event_type']}\n"
            f"Tenant: {event_row['tenant_id']}\n"
            f"Event ID: {event_row['event_id']}\n\n"
            f"Payload:\n{json.dumps(payload, indent=2, sort_keys=True)}\n"
        )
        html_content = (
            f"<html><body>"
            f"<h3>RelayCore Notification: {event_row['event_type']}</h3>"
            f"<p><b>Tenant:</b> {event_row['tenant_id']} | <b>Event ID:</b> {event_row['event_id']}</p>"
            f"<pre>{json.dumps(payload, indent=2, sort_keys=True)}</pre>"
            f"</body></html>"
        )
        msg.set_content(plain_text)
        msg.add_alternative(html_content, subtype="html")
        return msg, message_id

    def dispatch(
        self,
        event_row: Dict[str, Any],
        timeout_sec: float = settings.WORKER_HTTP_TIMEOUT_SEC,
    ) -> DeliveryResult:
        start_ns = time.perf_counter_ns()
        recipient = event_row["destination"].strip().lower()
        local_part, domain = recipient.split("@", 1)
        mime_msg, message_id = self.build_mime_message(event_row)

        transcript = [
            f"S: 220 mx.{domain} ESMTP RelayCore-MockMX Ready",
            "C: EHLO relaycore.internal",
            f"S: 250-mx.{domain} Hello relaycore.internal",
            "S: 250-STARTTLS",
            "C: STARTTLS",
            "S: 220 2.0.0 Ready to start TLS",
            f"C: MAIL FROM:<{self._sender}>",
            "S: 250 2.1.0 Sender OK",
            f"C: RCPT TO:<{recipient}>",
        ]

        if domain == "throttled-mx.example.com" or local_part.startswith("greylist"):
            transcript.append("S: 421 4.7.0 [TEMPFAIL] Too many concurrent connections from relaycore. Try again in 5s.")
            latency_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            return DeliveryResult(
                success=False,
                status_code=429,
                latency_ms=latency_ms,
                error_category="SMTP_421_GREYLISTED",
                response_excerpt=transcript[-1],
                retry_after_header=5.0,
                is_transient_failure=True,
                metadata={"smtp_transcript": transcript},
            )

        if domain == "mailbox-full.example.com" or local_part.startswith("softbounce"):
            transcript.append("S: 452 4.2.2 [SOFT_BOUNCE] Recipient mailbox is full; quota exceeded.")
            latency_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            return DeliveryResult(
                success=False,
                status_code=503,
                latency_ms=latency_ms,
                error_category="SMTP_452_MAILBOX_FULL",
                response_excerpt=transcript[-1],
                is_transient_failure=True,
                metadata={"smtp_transcript": transcript},
            )

        if domain == "invalid-domain.example.com" or local_part.startswith("hardbounce"):
            transcript.append(f"S: 550 5.1.1 [HARD_BOUNCE] <{recipient}>: Recipient address rejected: User unknown.")
            latency_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            return DeliveryResult(
                success=False,
                status_code=422,
                latency_ms=latency_ms,
                error_category="SMTP_550_HARD_BOUNCE",
                response_excerpt=transcript[-1],
                is_transient_failure=False,
                metadata={"smtp_transcript": transcript},
            )

        queue_id = uuid.uuid4().hex[:10].upper()
        transcript.extend(
            [
                "S: 250 2.1.5 Recipient OK",
                "C: DATA",
                "S: 354 Start mail input; end with <CRLF>.<CRLF>",
                f"C: [MIME multipart/alternative payload: {len(mime_msg.as_bytes())} bytes]",
                f"S: 250 2.0.0 OK: queued as {queue_id} (Message-ID: {message_id})",
                "C: QUIT",
                "S: 221 2.0.0 Bye",
            ]
        )

        self._spooled_messages.append(
            {
                "queue_id": queue_id,
                "message_id": message_id,
                "event_id": event_row["event_id"],
                "tenant_id": event_row["tenant_id"],
                "recipient": recipient,
                "subject": mime_msg["Subject"],
                "raw_mime": mime_msg.as_string(),
                "smtp_transcript": transcript,
            }
        )
        latency_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
        return DeliveryResult(
            success=True,
            status_code=250,
            latency_ms=latency_ms,
            response_excerpt=transcript[-3],
            is_transient_failure=False,
            metadata={"queue_id": queue_id, "message_id": message_id, "smtp_transcript": transcript},
        )


class DeliveryStrategyRegistry:
    def __init__(self, http_transport: Optional[HTTPTransportCallable] = None) -> None:
        self.webhook_strategy = WebhookHTTPDispatcher(transport=http_transport)
        self.slack_strategy = SlackBlockKitDispatcher(transport=http_transport)
        self.email_strategy = MockSMTPProtocolDispatcher()
        self._strategies: Dict[ChannelType, DeliveryStrategy] = {
            ChannelType.WEBHOOK: self.webhook_strategy,
            ChannelType.SLACK: self.slack_strategy,
            ChannelType.EMAIL: self.email_strategy,
        }

    def get_strategy(self, channel: ChannelType | str) -> DeliveryStrategy:
        ch = ChannelType(channel) if isinstance(channel, str) else channel
        if ch not in self._strategies:
            raise KeyError(f"Unsupported delivery channel: {ch}")
        return self._strategies[ch]
