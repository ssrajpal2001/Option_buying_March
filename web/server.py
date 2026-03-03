from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path

from web.config_api import router as config_router
from web.broker_api import router as broker_router
from web.status_api import router as status_router
from web.bot_control import router as bot_router

BASE_DIR = Path(__file__).parent

app = FastAPI(title="AlgoSoft Dashboard", version="1.0.0")

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app.include_router(config_router, prefix="/api")
app.include_router(broker_router, prefix="/api")
app.include_router(status_router, prefix="/api")
app.include_router(bot_router, prefix="/api")


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/strategy", response_class=HTMLResponse)
async def strategy_page(request: Request):
    return templates.TemplateResponse("strategy.html", {"request": request})


@app.get("/brokers", response_class=HTMLResponse)
async def brokers_page(request: Request):
    return templates.TemplateResponse("brokers.html", {"request": request})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("web.server:app", host="0.0.0.0", port=5000, reload=True)
