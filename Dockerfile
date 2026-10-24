FROM --platform=$BUILDPLATFORM golang:1.27.2-bookworm AS builder

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/scheduler-controller ./cmd/scheduler && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/scheduler-worker ./cmd/worker

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=builder /out/scheduler-controller /app/scheduler-controller
COPY --from=builder /out/scheduler-worker /app/scheduler-worker

USER 65532:65532
EXPOSE 8080

ENTRYPOINT ["/app/scheduler-controller"]
