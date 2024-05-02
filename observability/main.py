# standard libraries
from typing import Annotated

# observability-logs
from asgi_correlation_id import CorrelationIdMiddleware
from observability.logging_config import (
    configure_logger, 
    LogRequestResponseMiddleware
)

# observability-trace
from observability.tracing_config import (
    configure_tracer, 
    configure_meter, 
    propagate_telemetry_context, 
    extract_telemetry_context)

# fastapi imports
from fastapi import FastAPI, Request, HTTPException

logger = configure_logger(prod=False, hide_uvicorn_logger=False)
tracer = configure_tracer(cloud=False)

app = FastAPI()

app.add_middleware(LogRequestResponseMiddleware, logger=logger) # make cause a problem with the backgroundtasks
app.add_middleware(CorrelationIdMiddleware)

@app.get("/")
async def root(request: Request) -> str:
    return "ok"
