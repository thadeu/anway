FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build

ARG TARGETOS TARGETARCH VERSION=dev

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
  -ldflags "-s -w -X github.com/thadeu/anway/internal/config.Version=$VERSION" \
  -o /out/anway ./cmd/server

RUN mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot

ENV BIND_ADDR=0.0.0.0:7419 DATA_DIR=/data CONF_DIR=/etc/anway/conf.d

COPY --from=build /out/anway /usr/local/bin/anway
COPY --from=build --chown=65532:65532 /out/data /data

VOLUME /data
EXPOSE 7419 7420

ENTRYPOINT ["/usr/local/bin/anway"]
