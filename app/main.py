from contextlib import asynccontextmanager
import os
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, AsyncIterator, Dict, Optional
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from app.api.service_controller import RelayCoreApplicationController
from app.core.config import settings
from app.mocks.chaos_receiver import StatefulChaosTargetServer
from app.repositories.event_repository import TransactionalEventRepository
from app.schemas.event_schemas import EventIngestRequest, EventStatusResponse
from app.services.circuit_breaker import EndpointCircuitBreaker
from app.services.rate_limiter import TokenBucketRateLimiter

_sqlite_conn = sqlite3.connect(settings.SQLITE_FALLBACK_PATH, check_same_thread=False)
repository = TransactionalEventRepository(_sqlite_conn)
rate_limiter = TokenBucketRateLimiter()
circuit_breaker = EndpointCircuitBreaker()
chaos_server = StatefulChaosTargetServer()

controller = RelayCoreApplicationController(
    repository=repository,
    rate_limiter=rate_limiter,
    circuit_breaker=circuit_breaker,
    chaos_server=chaos_server,
)

_stop_worker = threading.Event()


def _background_worker_loop() -> None:
    poll_sec = float(os.getenv("WORKER_POLL_INTERVAL_SEC", "3.0"))
    while not _stop_worker.is_set():
        try:
            controller.run_worker_tick_endpoint(batch_size=settings.WORKER_BATCH_SIZE)
        except Exception:
            pass
        _stop_worker.wait(poll_sec)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    enable_embedded = os.getenv("ENABLE_EMBEDDED_WORKER", "true").lower() == "true"
    worker_thread: Optional[threading.Thread] = None
    if enable_embedded:
        _stop_worker.clear()
        worker_thread = threading.Thread(
            target=_background_worker_loop,
            name="relaycore-embedded-worker",
            daemon=True,
        )
        worker_thread.start()
    yield
    _stop_worker.set()


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    lifespan=lifespan,
    description=(
        "Fault-Tolerant Multi-Tenant Asynchronous Webhook & Notification Delivery Engine "
        "featuring Transactional Polling (`FOR UPDATE SKIP LOCKED`), HMAC-SHA256 Signing, "
        "Token-Bucket Rate Limiting, 3-State Circuit Breakers, and Dead-Letter Queue (DLQ) Replay."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChaosModeUpdateRequest(BaseModel):
    route_path: str = "/mock-target/outage"
    mode: str = "HEALTHY_200"


@app.get("/", response_class=HTMLResponse, tags=["UI Console"])
def serve_operations_dashboard() -> HTMLResponse:
    dashboard_path = Path(__file__).resolve().parent / "dashboard.html"
    if not dashboard_path.exists():
        return HTMLResponse("<h3>dashboard.html not found in app/ directory</h3>", status_code=404)
    return HTMLResponse(content=dashboard_path.read_text(encoding="utf-8"))


@app.get("/health", tags=["System"])
def health_check() -> Dict[str, str]:
    return {"status": "healthy", "service": settings.PROJECT_NAME, "version": settings.VERSION}


@app.get("/v1/events", tags=["Events"])
def list_recent_events(limit: int = Query(default=50, ge=1, le=200)) -> Dict[str, Any]:
    with repository._lock:
        cur = repository._conn.cursor()
        cur.execute(
            "SELECT event_id FROM delivery_events ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )
        event_ids = [row["event_id"] for row in cur.fetchall()]
    items = [repository.get_event_by_id(eid).model_dump(mode="json") for eid in event_ids]
    return {"count": len(items), "items": items}


@app.post("/v1/events", response_model=EventStatusResponse, tags=["Events"])
def ingest_event(req: EventIngestRequest, response: Response) -> EventStatusResponse:
    status_code, event_resp = controller.ingest_event_endpoint(req)
    response.status_code = status_code
    return event_resp


@app.get("/v1/events/{event_id}", tags=["Events"])
def get_event_status(event_id: str) -> Dict[str, Any]:
    status_code, body = controller.get_event_endpoint(event_id)
    if status_code == 404:
        raise HTTPException(status_code=404, detail=body)
    return body


@app.get("/v1/dlq", tags=["Dead-Letter Queue (DLQ)"])
def list_dlq_events(tenant_id: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    _, body = controller.list_dlq_endpoint(tenant_id=tenant_id)
    return body


@app.post("/v1/dlq/{event_id}/replay", tags=["Dead-Letter Queue (DLQ)"])
def replay_dlq_event(event_id: str) -> Dict[str, Any]:
    status_code, body = controller.replay_single_dlq_endpoint(event_id)
    if status_code != 200:
        raise HTTPException(status_code=status_code, detail=body)
    return body


@app.post("/v1/dlq/replay-batch", tags=["Dead-Letter Queue (DLQ)"])
def replay_dlq_batch(tenant_id: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    _, body = controller.replay_bulk_dlq_endpoint(tenant_id=tenant_id)
    return body


@app.post("/v1/workers/run-once", tags=["Worker Orchestration"])
def trigger_worker_batch(batch_size: int = Query(default=25, ge=1, le=100)) -> Dict[str, Any]:
    _, body = controller.run_worker_tick_endpoint(batch_size=batch_size)
    return body


@app.get("/v1/observability/dashboard", tags=["Observability & Chaos Testing"])
def get_observability_dashboard() -> Dict[str, Any]:
    _, body = controller.get_observability_dashboard()
    body["smtp_spooled_messages"] = controller.strategy_registry.email_strategy.spooled_messages
    return body


@app.post("/v1/chaos/configure", tags=["Observability & Chaos Testing"])
def configure_chaos_route(req: ChaosModeUpdateRequest) -> Dict[str, Any]:
    _, body = controller.configure_chaos_route_endpoint(req.route_path, req.mode)
    return body


@app.post("/mock-target/{route_name}", tags=["Observability & Chaos Testing"])
async def mock_partner_webhook_receiver(route_name: str, request: Request) -> Response:
    raw_body = await request.body()
    headers = {k: v for k, v in request.headers.items()}
    status_code, resp_text, resp_headers = chaos_server.handle_request(
        url=f"http://localhost:8000/mock-target/{route_name}",
        body_bytes=raw_body,
        headers=headers,
    )
    return Response(content=resp_text, status_code=status_code, headers=resp_headers, media_type="application/json")
