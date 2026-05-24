# Multi-stage build for the python_sandbox_agent gRPC server.
#
# Stage 1 (golang): cross-compile the Go binary statically so it runs in
# a distroless container.
# Stage 2 (python:3.12-slim): provides /usr/bin/python3 for the executor
# subprocess. Distroless lacks python.
#
# Image must be a child of a Python base because the agent shells out to
# python3 to execute user code. The surrounding agent-sandbox Pod (gVisor)
# provides the actual isolation; this container just runs python locally.

FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags='-s -w' \
      -o /out/agent ./examples/python_sandbox_agent

FROM python:3.12-slim
COPY --from=build /out/agent /usr/local/bin/agent
USER 1000:1000
EXPOSE 8494
ENTRYPOINT ["/usr/local/bin/agent"]
