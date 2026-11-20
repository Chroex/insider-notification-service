-- W3C traceparent of the request that created the notification. The worker
-- continues that trace for each delivery attempt, so one trace spans
-- API -> DB -> worker -> provider despite the asynchronous queue in between.
ALTER TABLE notifications ADD COLUMN trace_parent text;
