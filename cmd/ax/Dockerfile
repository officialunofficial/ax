# Dockerfile for the ax-server binary. Provides a non-ko build path so we
# can use Cloud Build without installing ko in the builder.
#
# Build context: AX repo root.
#   gcloud builds submit --config=cmd/ax/cloudbuild.yaml \
#                        --substitutions=_TAG=$(git rev-parse --short HEAD) .

FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags='-s -w' \
      -o /out/ax ./cmd/ax

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ax /usr/local/bin/ax
USER nonroot:nonroot
EXPOSE 8494
ENTRYPOINT ["/usr/local/bin/ax", "serve"]
