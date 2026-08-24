# syntax=docker/dockerfile:1
# Sandbox worker: dynamically spawned by ops-extension (client-go) per session.
ARG REGISTRY=rucoder-artifact.temp.10.199.64.20.nip.io
FROM ${REGISTRY}/golang:1.26-alpine AS build
ARG HTTP_PROXY=http://mihomo.develop.svc.cluster.local:7890
ARG HTTPS_PROXY=http://mihomo.develop.svc.cluster.local:7890
ENV HTTP_PROXY=${HTTP_PROXY} \
    HTTPS_PROXY=${HTTPS_PROXY} \
    NO_PROXY=localhost,127.0.0.1,.svc.cluster.local,.svc,.nip.io
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/worker-go .

FROM ${REGISTRY}/alpine:3.24
RUN apk add --no-cache ca-certificates
COPY --from=build /out/worker-go /usr/local/bin/worker-go
EXPOSE 8080
ENTRYPOINT ["worker-go"]
