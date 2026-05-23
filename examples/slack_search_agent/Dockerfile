# Multi-stage build for the slack_search_agent gRPC server.
#
# Stage 1 (golang): cross-compile the Go binary statically.
# Stage 2 (distroless/static-debian12:nonroot): minimal runtime — this
# agent only makes outbound HTTPS calls to slack.com, no subprocess
# execution required, so distroless static is sufficient.

FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags='-s -w' \
      -o /out/agent ./examples/slack_search_agent

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/agent /usr/local/bin/agent
USER nonroot:nonroot
EXPOSE 8494
ENTRYPOINT ["/usr/local/bin/agent"]
