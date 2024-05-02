# trace
from typing import Any
from opentelemetry import trace, baggage
from opentelemetry.propagate import set_global_textmap, Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.propagators.cloud_trace_propagator import CloudTraceFormatPropagator

# trace-propagations
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.baggage.propagation import W3CBaggagePropagator

# metrics
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader, ConsoleMetricExporter
from opentelemetry.exporter.cloud_monitoring import CloudMonitoringMetricsExporter


def configure_tracer(cloud=False) -> trace.Tracer:
    
    # Using the X-Cloud-Trace-Context header
    if cloud: set_global_textmap(CloudTraceFormatPropagator())
    
    exporter = (CloudTraceSpanExporter if cloud else ConsoleSpanExporter)
    
    provider = TracerProvider()
    processor = BatchSpanProcessor(exporter())
    provider.add_span_processor(processor)
    
    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer(__file__)
    return tracer

def configure_meter(cloud=False) -> metrics.Meter:
    
    metric_reader = PeriodicExportingMetricReader(ConsoleMetricExporter())
    provider = MeterProvider(metric_readers=[metric_reader])
    
    metrics.set_meter_provider(provider)
    meter = metrics.get_meter(__file__)
    return meter

def propagate_telemetry_context(info: dict, ctx: Context = None) -> dict:
    
    headers = {}
    ctx = baggage.clear() if ctx is None else ctx
    for key, value in info.items():
        ctx = baggage.set_baggage(key, value, ctx)
        
    W3CBaggagePropagator().inject(headers, ctx)
    TraceContextTextMapPropagator().inject(headers, ctx)
    
    return headers

def extract_telemetry_context(headers: dict) -> Context:
    carrier = dict(traceparent=headers["traceparent"])
    ctx = TraceContextTextMapPropagator().extract(carrier)
    
    b2 = dict(baggage=headers["baggage"])
    ctx2 = W3CBaggagePropagator().extract(b2, context=ctx)
    
    return ctx2