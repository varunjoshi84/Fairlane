import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from fairlane.api.routes import router
from fairlane.api.dlq_routes import dlq_router
from fairlane.api.tenant_routes import tenant_router
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
from fairlane.config import settings
from fairlane.db import init_db
from fairlane.logging_setup import setup_logging

logger = logging.getLogger("fairlane.api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Lifespan event handler for startup and shutdown procedures."""
    setup_logging(settings.log_level)
    logger.info("Starting Fairlane API service...")
    try:
        await init_db()
    except Exception as e:
        logger.warning(f"Database init check at startup: {e}")
    yield
    logger.info("Shutting down Fairlane API service...")


app = FastAPI(
    title="Fairlane API",
    description="API for managing background tasks in the Fairlane engine",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")


app.include_router(router)
app.include_router(dlq_router)
app.include_router(tenant_router)

@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("fairlane.api.main:app", host=settings.api_host, port=settings.api_port, reload=True)
