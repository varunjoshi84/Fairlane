from fastapi import FastAPI
from fairlane.api.routes import router

app = FastAPI(
    title="Fairlane API",
    description="API for managing background tasks in the Fairlane engine",
    version="0.1.0"
)

app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    from fairlane.config import settings
    uvicorn.run("fairlane.api.main:app", host=settings.api_host, port=settings.api_port, reload=True)
