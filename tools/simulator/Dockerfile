FROM golang:1.24.13-bookworm AS codecs
RUN apt-get update && apt-get install -y --no-install-recommends build-essential autoconf automake libtool pkg-config curl ca-certificates libopus-dev libopusfile-dev nasm && rm -rf /var/lib/apt/lists/*
WORKDIR /tmp
RUN curl -fsSL https://github.com/mstorsjo/fdk-aac/archive/refs/tags/v2.0.3.tar.gz | tar xz && cd fdk-aac-2.0.3 && autoreconf -fiv && ./configure --prefix=/usr/local && make -j4 && make install && ldconfig
RUN curl -fsSL https://ffmpeg.org/releases/ffmpeg-7.1.1.tar.xz | tar xJ && cd ffmpeg-7.1.1 && ./configure --enable-gpl --enable-nonfree --enable-libfdk-aac --enable-libopus --disable-doc --disable-debug --disable-ffplay && make -j4 && make install
ENV LD_LIBRARY_PATH=/usr/local/lib
FROM codecs AS go2rtc-builder
RUN CGO_ENABLED=0 go install github.com/AlexxIT/go2rtc@v1.9.14
FROM codecs AS build
WORKDIR /src
COPY tools/simulator/go.mod tools/simulator/go.sum ./
RUN go mod download
COPY tools/simulator/ ./
RUN go build -o /usr/local/bin/simulator .
COPY --from=go2rtc-builder /go/bin/go2rtc /go/bin/go2rtc
FROM build AS test
CMD ["go", "test", "-race", "-timeout=2m", "./..."]
FROM build AS runtime
ENTRYPOINT ["simulator"]
