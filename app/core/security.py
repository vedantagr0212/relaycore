import hashlib
import hmac
import time
from typing import Tuple


class WebhookSignatureError(ValueError):
    """Raised when a webhook signature is missing, malformed, expired, or invalid."""


def generate_webhook_signature(
    payload_body: bytes,
    secret_key: str,
    timestamp: int | None = None,
) -> Tuple[str, int]:
    """
    Computes a Stripe/Razorpay-grade HMAC-SHA256 signature header.
    Format: t=<unix_timestamp>,v1=<hex_digest>
    Signed message: f"{timestamp}.{payload_body_utf8}"
    """
    if not secret_key:
        raise ValueError("Webhook signing secret_key must not be empty.")
    ts = timestamp if timestamp is not None else int(time.time())
    signed_payload = f"{ts}.".encode("utf-8") + payload_body
    digest = hmac.new(
        key=secret_key.encode("utf-8"),
        msg=signed_payload,
        digestmod=hashlib.sha256,
    ).hexdigest()
    return f"t={ts},v1={digest}", ts


def verify_webhook_signature(
    payload_body: bytes,
    signature_header: str,
    secret_key: str,
    tolerance_seconds: int = 300,
    current_timestamp: int | None = None,
) -> bool:
    """
    Verifies the HMAC-SHA256 signature using constant-time digest comparison
    and enforces a sliding timestamp window to prevent replay attacks.
    """
    if not signature_header or not secret_key:
        raise WebhookSignatureError("Missing signature header or secret key.")

    parts = {}
    for item in signature_header.split(","):
        if "=" in item:
            k, v = item.strip().split("=", 1)
            parts[k] = v

    if "t" not in parts or "v1" not in parts:
        raise WebhookSignatureError("Malformed signature header: expected 't=<ts>,v1=<sig>'.")

    try:
        ts = int(parts["t"])
    except ValueError as exc:
        raise WebhookSignatureError("Invalid timestamp in signature header.") from exc

    now = current_timestamp if current_timestamp is not None else int(time.time())
    if abs(now - ts) > tolerance_seconds:
        raise WebhookSignatureError(
            f"Webhook timestamp {ts} outside tolerance window ({tolerance_seconds}s). Possible replay attack."
        )

    expected_header, _ = generate_webhook_signature(payload_body, secret_key, timestamp=ts)
    expected_sig = expected_header.split(",v1=")[1]

    if not hmac.compare_digest(expected_sig, parts["v1"]):
        raise WebhookSignatureError("HMAC-SHA256 signature mismatch.")

    return True
