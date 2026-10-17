# Community images are no longer publicly available; build official fixed sources.
FROM golang:1.24.2-bookworm AS builder
ENV CGO_ENABLED=0 GOMAXPROCS=2
RUN go install -p 2 github.com/minio/minio@RELEASE.2025-04-22T22-12-26Z
RUN go install -p 2 github.com/minio/mc@RELEASE.2025-04-16T18-13-26Z
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /go/bin/minio /usr/local/bin/minio
COPY --from=builder /go/bin/mc /usr/local/bin/mc
ENTRYPOINT ["minio"]
