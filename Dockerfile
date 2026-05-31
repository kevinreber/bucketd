# syntax=docker/dockerfile:1.6
#
# Multi-stage build for bucketd. Two stages:
#   1. `builder` — has the Go toolchain; compiles the static binary
#   2. `final`   — distroless base, ~5 MB layer + ~25 MB binary
#
# Why distroless: no shell, no package manager, no userland → tiny attack
# surface. CVE scanners report ~zero findings on `gcr.io/distroless/static`
# because there's nothing to scan.

# ---- builder ----
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Cache deps. Copying go.mod/go.sum separately lets Docker reuse the
# module-download layer across iteration cycles that don't change deps.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static binary: no cgo, no glibc dependency. Strip debug info to shave
# ~5 MB off the final image. `-trimpath` removes filesystem paths from
# the binary for reproducibility.
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/bucketd \
    ./cmd/server

# ---- final ----
FROM gcr.io/distroless/static:nonroot

# Default ports — overridable via env at runtime.
ENV ADDR=:50051
ENV HTTP_ADDR=:8080
EXPOSE 50051
EXPOSE 8080

# Run as the `nonroot` user provided by the distroless image (UID 65532).
# No shell, no package manager, no userland to exploit.
USER nonroot:nonroot

COPY --from=builder /out/bucketd /usr/local/bin/bucketd

ENTRYPOINT ["/usr/local/bin/bucketd"]
