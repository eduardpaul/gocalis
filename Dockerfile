# Stage 1: Build the React dashboard
FROM node:22-alpine AS web-builder
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm install
COPY web/ ./
RUN npm run build

# Stage 2: Build and run the Go application
FROM golang:1.24-bookworm

# Install basic development tools, alsa-utils for sound, and libopus(+file)
# dev headers + pkg-config for the CGO Opus wideband transport (T18).
RUN apt-get update && apt-get install -y \
    alsa-utils \
    bzip2 \
    libopus-dev \
    libopusfile-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dashboard build output so the Go embed can find it
COPY --from=web-builder /web/dist ./internal/webserver/dist

# Run a simple build or keep alive command
CMD ["go", "run", "cmd/main.go"]
