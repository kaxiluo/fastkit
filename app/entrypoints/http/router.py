"""HTTP v1 版本聚合入口。所有 v1 module router 挂到这里。"""

from __future__ import annotations

from fastapi import APIRouter

from app.modules.example.router import router as example_router

router = APIRouter(prefix="/api/v1")
router.include_router(example_router)
