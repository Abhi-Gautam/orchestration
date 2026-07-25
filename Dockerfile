# Builds either cmd/web or cmd/worker.
#   docker build --build-arg COMMAND=web -t orchestration-web .
#   docker build --build-arg COMMAND=worker -t orchestration-worker .
ARG GO_VERSION=1.26

FROM golang:${GO_VERSION}-bookworm AS base
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

FROM base AS build
ARG COMMAND=web
RUN test "$COMMAND" = "web" || test "$COMMAND" = "worker" \
  && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app "./cmd/${COMMAND}"

FROM base AS development
RUN go install github.com/air-verse/air@v1.67.2
ENTRYPOINT ["air"]

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
