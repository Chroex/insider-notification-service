package observability

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// TracerName is the instrumentation scope for this service's own spans.
const TracerName = "notification-service"

// Tracer returns the service tracer (a no-op until SetupTracing enables it).
func Tracer() trace.Tracer { return otel.Tracer(TracerName) }

// SetupTracing installs the global tracer provider and W3C propagator.
//
// Tracing is enabled only when OTEL_EXPORTER_OTLP_ENDPOINT is set (standard
// OpenTelemetry variable; compose points it at Jaeger). The exporter reads
// the other standard OTEL_EXPORTER_OTLP_* variables itself. Without the
// endpoint everything stays a no-op, so there is no overhead or noise
// locally or in tests.
func SetupTracing(ctx context.Context, service string) (shutdown func(context.Context) error, err error) {
	// The propagator is needed even without export: the API still writes
	// traceparent onto rows, so a worker with tracing enabled can continue
	// the trace.
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}
	res, err := resource.Merge(resource.Default(),
		resource.NewSchemaless(attribute.String("service.name", service)))
	if err != nil {
		return nil, fmt.Errorf("tracing resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(NoOrphanClientSpans{})),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// NoOrphanClientSpans is the root sampler: it records root spans of every
// kind except Client.
//
// pgx and Redis instrumentation create Client spans for every query. When
// that query belongs to a request or a delivery it has a parent, and
// ParentBased samples it with its trace. Client spans with no parent are the
// worker's background chatter: claim polls every 500ms per channel, rate
// limiter calls, reaper and queue-depth queries. Recording each one as its own
// trace would bury the useful traces in Jaeger.
type NoOrphanClientSpans struct{}

func (NoOrphanClientSpans) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	decision := sdktrace.RecordAndSample
	if p.Kind == trace.SpanKindClient {
		decision = sdktrace.Drop
	}
	return sdktrace.SamplingResult{
		Decision:   decision,
		Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState(),
	}
}

func (NoOrphanClientSpans) Description() string { return "NoOrphanClientSpans" }

// TraceParent returns the W3C traceparent of the span in ctx, for storing on
// a notification row. nil when ctx carries no sampled span.
func TraceParent(ctx context.Context) *string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if v, ok := carrier["traceparent"]; ok && v != "" {
		return &v
	}
	return nil
}

// ContextWithTraceParent returns ctx carrying the remote span context from a
// stored traceparent, so a span started from it continues that trace.
func ContextWithTraceParent(ctx context.Context, traceParent *string) context.Context {
	if traceParent == nil || *traceParent == "" {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": *traceParent})
}
