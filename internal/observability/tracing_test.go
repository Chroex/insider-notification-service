package observability

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestNoOrphanClientSpans(t *testing.T) {
	sampler := sdktrace.ParentBased(NoOrphanClientSpans{})
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler))
	tracer := tp.Tracer("test")
	ctx := context.Background()

	_, orphanClient := tracer.Start(ctx, "SELECT (claim poll)", trace.WithSpanKind(trace.SpanKindClient))
	if orphanClient.SpanContext().IsSampled() {
		t.Error("a root client span (background DB/Redis call) must be dropped")
	}
	parentCtx, server := tracer.Start(ctx, "POST /api/v1/notifications", trace.WithSpanKind(trace.SpanKindServer))
	if !server.SpanContext().IsSampled() {
		t.Error("a root server span must be sampled")
	}
	_, child := tracer.Start(parentCtx, "INSERT", trace.WithSpanKind(trace.SpanKindClient))
	if !child.SpanContext().IsSampled() {
		t.Error("a client span inside a sampled request must be sampled")
	}
	_, consumer := tracer.Start(ctx, "deliver sms", trace.WithSpanKind(trace.SpanKindConsumer))
	if !consumer.SpanContext().IsSampled() {
		t.Error("a root consumer span (delivery without stored context) must be sampled")
	}
}

func TestTraceParentRoundTrip(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("test").Start(context.Background(), "create")
	defer span.End()

	tp1 := TraceParent(ctx)
	if tp1 == nil {
		t.Fatal("no traceparent for a sampled span")
	}
	restored := trace.SpanContextFromContext(ContextWithTraceParent(context.Background(), tp1))
	if restored.TraceID() != span.SpanContext().TraceID() || restored.SpanID() != span.SpanContext().SpanID() || !restored.IsRemote() {
		t.Errorf("restored %v, want remote %v", restored, span.SpanContext())
	}

	if TraceParent(context.Background()) != nil {
		t.Error("no span -> no traceparent")
	}
	if got := ContextWithTraceParent(context.Background(), nil); trace.SpanContextFromContext(got).IsValid() {
		t.Error("nil traceparent must leave the context alone")
	}
}
