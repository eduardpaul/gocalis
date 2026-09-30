FROM node:24.15.0-bookworm-slim AS web-builder
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM golang:1.24.13-bookworm AS go-base
RUN apt-get update && apt-get install -y --no-install-recommends \
    alsa-utils libopus-dev libopusfile-dev pkg-config bzip2 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app

FROM go-base AS builder
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
COPY scripts/ scripts/
COPY config.yaml ./
COPY --from=web-builder /web/dist internal/webserver/dist
RUN go build -trimpath -o /out/gocalis ./cmd
# The sherpa Go package links native libraries from its module cache. Copy just
# the libraries for this architecture so the runtime never needs the Go cache.
RUN mkdir -p /out/lib && \
    case "$(go env GOARCH)" in \
      amd64) native_arch=x86_64-unknown-linux-gnu ;; \
      arm64) native_arch=aarch64-unknown-linux-gnu ;; \
      arm) native_arch=arm-unknown-linux-gnueabihf ;; \
      *) exit 1 ;; \
    esac && \
    native_module_dir=$(go list -m -f '{{.Dir}}' github.com/k2-fsa/sherpa-onnx-go-linux) && \
    cp "$native_module_dir/lib/$native_arch/"*.so /out/lib/

FROM builder AS test
CMD ["go", "test", "-race", "-timeout=2m", "./..."]

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    alsa-utils ca-certificates libopus0 libopusfile0 libgomp1 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /out/gocalis /usr/local/bin/gocalis
COPY --from=builder /out/lib/ /opt/gocalis/lib/
COPY config.yaml /app/config.yaml
ENV LD_LIBRARY_PATH=/opt/gocalis/lib
EXPOSE 8080 9090
ENTRYPOINT ["gocalis"]
