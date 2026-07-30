from fastapi import FastAPI

from app.bootstrap.api import api_lifespan, setup_api
from app.config.settings import get_app_settings
from app.entrypoints.http.exception_handlers import register_exception_handlers
from app.entrypoints.http.health import router as health_router
from app.entrypoints.http.router import router as v1_router

settings = get_app_settings()

app = FastAPI(title=settings.app_name, lifespan=api_lifespan)
# 注册 dishka ContainerMiddleware,必须在任何请求前完成
setup_api(app)

register_exception_handlers(app)
app.include_router(health_router)
app.include_router(v1_router)
