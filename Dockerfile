# The browser control plane as one static binary.
#
# The whole cluster is goroutines in one process over the in-process
# transport, so this image is the entire system: no sidecars, no database, no
# certificates. That is what lets it run on a free tier. The bank/gateway/
# client programs are not part of the image.

FROM golang:1.25-alpine AS build
WORKDIR /src

# Dependencies first, so a source-only change reuses the module cache layer.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/cluster ./cmd/cluster

FROM scratch
COPY --from=build /out/cluster /cluster

# Render and Fly inject PORT and expect the process to bind it; the default is
# for a bare `docker run`.
ENV PORT=8080 TRANSPORT=inproc
EXPOSE 8080
ENTRYPOINT ["/cluster"]
