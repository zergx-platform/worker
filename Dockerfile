# syntax=docker/dockerfile:1
# Sandbox worker: dynamically spawned by ops-extension (client-go) per session.
#
# Debian trixie base (glibc + real coreutils) instead of alpine/musl: debian
# ships `/bin/pwd`, `/bin/ls`, `/bin/echo` as real glibc binaries, avoiding the
# alpine-busybox dynamic-loader ENONT /bin/<cmd> exec failures seen in the
# sandbox. The Debian mirrors are pre-configured to Aliyun for fast in-cluster
# package pulls.
ARG REGISTRY=artifact.temp.svc.cluster.local
# Build stage stays on golang:1.26-alpine: the trixie golang image is a
# buildpack-deps variant whose ~102MB base layer cannot be pulled reliably
# through the mihomo→docker.io egress (it truncates mid-download). Only the
# RUNTIME image below switches to debian (glibc + real coreutils), which is
# what fixes the sandbox /bin/<cmd> exec failures.
FROM ${REGISTRY}/library/golang:1.26-alpine AS build
ARG HTTP_PROXY=http://mihomo.develop.svc.cluster.local:7890
ARG HTTPS_PROXY=http://mihomo.develop.svc.cluster.local:7890
ENV HTTP_PROXY=${HTTP_PROXY} \
    HTTPS_PROXY=${HTTPS_PROXY} \
    NO_PROXY=localhost,127.0.0.1,.svc.cluster.local,.svc,.nip.io \
    GOINSECURE=forgejo.develop.10.199.64.20.nip.io \
    GOPRIVATE=forgejo.develop.10.199.64.20.nip.io \
    GOPROXY=http://artifact.zergx.svc.cluster.local/pkgs/go \
    GOSUMDB=off \
    GOFLAGS=-mod=mod
RUN sed -i 's|dl-cdn.alpinelinux.org|mirrors.aliyun.com|g' /etc/apk/repositories \
    && apk add --no-cache git \
    && git config --global http.sslVerify false \
    && git config --global url."https://root:devpassword@forgejo.develop.10.199.64.20.nip.io/".insteadOf "https://forgejo.develop.10.199.64.20.nip.io/"
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/worker-go .

FROM ${REGISTRY}/library/debian:trixie-slim
# Pre-configure Aliyun mirrors for the sandbox (agents also `apt install` tools
# inside sandboxes at runtime, e.g. go/gcc/build deps).
RUN sed -i -E 's#https?://deb\.debian\.org/debian#http://mirrors.aliyun.com/debian#g' /etc/apt/sources.list.d/debian.sources 2>/dev/null || true; \
    sed -i -E 's#https?://deb\.debian\.org/debian#http://mirrors.aliyun.com/debian#g' /etc/apt/sources.list 2>/dev/null || true; \
    sed -i -E 's#https?://security\.debian\.org#http://mirrors.aliyun.com/debian-security#g' /etc/apt/sources.list.d/debian.sources 2>/dev/null || true; \
    apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/worker-go /usr/local/bin/worker-go
EXPOSE 48080
ENTRYPOINT ["worker-go"]
