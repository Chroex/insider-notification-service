# syntax=docker/dockerfile:1

# ---- build stage: compile all binaries once -------------------------------
FROM golang:1.27-alpine AS build
WORKDIR /src

# Download modules in their own layer so code edits don't bust the cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO disabled -> fully static binaries that run on a distroless/static base.
# -trimpath and -s -w strip local paths and debug symbols for smaller images.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# ---- runtime stages: one small image per binary ---------------------------
# No shell, no package manager, non-root. Each image contains only its own
# binary; the build stage above is shared, so Go compiles once.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime
USER nonroot:nonroot

FROM runtime AS api
COPY --from=build /out/api /usr/local/bin/api
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/api"]

FROM runtime AS worker
COPY --from=build /out/worker /usr/local/bin/worker
EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/worker"]

FROM runtime AS mockprovider
COPY --from=build /out/mockprovider /usr/local/bin/mockprovider
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/mockprovider"]
