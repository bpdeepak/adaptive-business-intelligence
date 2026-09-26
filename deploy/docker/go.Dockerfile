# Go services of the ABI stack (Phase 5 deploy). One Dockerfile, two binaries:
#   docker build -f deploy/docker/go.Dockerfile --build-arg CMD=server   -t abi-server .
#   docker build -f deploy/docker/go.Dockerfile --build-arg CMD=producer -t abi-producer .
# Build context is the REPO ROOT so the server image can carry the governance
# policy (config/playbooks.yml); the dashboard is already embedded in the binary.
# Multi-arch: build on (or for) linux/arm64 for the Oracle Ampere VM.
FROM golang:1.27-alpine AS build
ARG CMD=server
WORKDIR /src/api
COPY api/go.mod api/go.sum ./
RUN go mod download
COPY api/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/app /app/app
# The server resolves ABI_PLAYBOOK_PATH (default config/playbooks.yml) relative
# to the working directory; a missing or invalid policy aborts boot.
COPY config/playbooks.yml /app/config/playbooks.yml
EXPOSE 8080 8090
ENTRYPOINT ["/app/app"]
