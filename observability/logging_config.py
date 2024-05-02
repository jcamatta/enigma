import sys
import logging
import structlog
from typing import Any, Callable

from asgi_correlation_id import correlation_id
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from google.cloud import logging as gcplogging

def _add_correlation(
    logger: logging.Logger, 
    method_name: str, 
    event_dict: dict[str, Any]) -> dict[str, Any]:
    
    """Add request id to log message."""
    if request_id := correlation_id.get():
        event_dict["request_id"] = request_id
    return event_dict

def configure_logger(prod: bool=True, hide_uvicorn_logger: bool=True) -> Any:
    
    if hide_uvicorn_logger:
        logging.getLogger("uvicorn.access").disabled = True
        logging.getLogger("uvicorn.error").disabled = True
        logging.getLogger("uvicorn").disabled = True
        
    shared_processors = [
            # _add_correlation,
            structlog.contextvars.merge_contextvars,
            structlog.processors.EventRenamer("text"),
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M.%S", utc=True),
            structlog.processors.add_log_level,
            structlog.processors.format_exc_info,
            structlog.processors.StackInfoRenderer(), 
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.CallsiteParameterAdder(
                {
                    # algunos de estos pierden el sentido cuando se usan ainfo awarning, etc.
                    # structlog.processors.CallsiteParameter.PATHNAME,
                    # structlog.processors.CallsiteParameter.FILENAME,
                    # structlog.processors.CallsiteParameter.LINENO,
                    # structlog.processors.CallsiteParameter.MODULE,
                    # structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.THREAD,
                    # structlog.processors.CallsiteParameter.THREAD_NAME,
                    structlog.processors.CallsiteParameter.PROCESS,
                    # structlog.processors.CallsiteParameter.PROCESS_NAME,
                }
            ),
    ]
    
    json_logs_render = (
        structlog.processors.JSONRenderer() if prod
        else structlog.dev.ConsoleRenderer(colors=True)
    )
    
    shared_processors.append(json_logs_render)

    structlog.configure(
        cache_logger_on_first_use=prod,
        processors=shared_processors,
        # logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
    )
    
    logger = structlog.stdlib.get_logger()
    return logger
    

class LogRequestResponseMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, logger: logging.Logger):
        super().__init__(app)
        self.logger = logger
    
    async def dispatch(self, request: Request, call_next: Callable):
        
        # clean
        structlog.contextvars.clear_contextvars()
        
        request_context = dict(
            http_method=request.method,
            http_scheme=request.url.scheme,
            url_host=request.headers.get("host"),
            url_path=request.url.path,
            user_agent=request.headers.get("user-agent"),
            request_id=request.headers.get("x-request-id"),
            client=f"{request.client.host}:{request.client.port}",
        )
        structlog.contextvars.bind_contextvars(
            request=request_context
        )
        
        
        # response
        response: Response = await call_next(request)
        
        response_context = dict(
            status_code=response.status_code
        )
        
        structlog.contextvars.bind_contextvars(
            response=response_context
        )
        
        return response