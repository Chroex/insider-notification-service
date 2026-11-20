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

# ---- runtime stage: no shell, no package manager, non-root -----------------
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot:nonroot
# Each compose service picks its binary via `command`.
ENTRYPOINT []
