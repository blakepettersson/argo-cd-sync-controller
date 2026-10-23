# Builds the controller on the build platform for the target one, and runs it on distroless as a non-root user.
# The Go image is the one argo-cd's Dockerfile pins.
FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.27.2@sha256:5bc7f572bbaa98885a3a1fd9c0aa76b59e3e14e8628bfc316bbfd0c701e4818c AS builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -o /argo-cd-sync-controller ./cmd

FROM gcr.io/distroless/static:nonroot@sha256:d62067879de890a1def9da0e7766b7d20810a5e5555cef8506b752437d7a3e88
COPY --from=builder /argo-cd-sync-controller /argo-cd-sync-controller
USER 65532:65532
ENTRYPOINT ["/argo-cd-sync-controller"]
