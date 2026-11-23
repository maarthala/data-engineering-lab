# MinIO client (mc) for every platform (linux/amd64 + linux/arm64), built from its source tag.
# MinIO stopped publishing mc (Docker Hub images removed, dl.minio.io → 410 Gone), and our old
# mirror was amd64 only — on Apple Silicon it ran emulated and storage-init never got through.
# The lab uses mc against RustFS: storage-init (demo bucket) and, copied into the Airflow,
# JupyterHub and cdc-runner images, per-member buckets / policies / quotas.
#
# Build + push (each platform, then one tag for both; Docker's default builder can't do both at once):
#   T=ghcr.io/maarthala/de-stack/mc:RELEASE.2025-08-13T08-35-41Z-multiarch
#   for p in amd64 arm64; do docker buildx build --platform linux/$p -f common/dockerfiles/Dockerfile.mc \
#     -t $T-$p --load common/dockerfiles && docker push $T-$p; done
#   docker buildx imagetools create -t $T $T-amd64 $T-arm64
ARG RELEASE=RELEASE.2025-08-13T08-35-41Z
ARG COMMIT=d6541ea280b73a834b64d4097e21f2be77676104

FROM --platform=$BUILDPLATFORM golang:1.23 AS build
ARG RELEASE
ARG COMMIT
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
RUN git init -q . && git fetch -q --depth 1 https://github.com/minio/mc "$COMMIT" && git checkout -q FETCH_HEAD
# same flags as upstream's buildscripts/gen-ldflags.go (version = the release's timestamp)
RUN VERSION=$(echo "$RELEASE" | sed -E 's/RELEASE\.([0-9-]+)T([0-9]+)-([0-9]+)-([0-9]+)Z/\1T\2:\3:\4Z/') && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -tags kqueue -o /out/mc \
      -ldflags "-s -w -X github.com/minio/mc/cmd.Version=$VERSION -X github.com/minio/mc/cmd.CopyrightYear=2025 \
        -X github.com/minio/mc/cmd.ReleaseTag=$RELEASE -X github.com/minio/mc/cmd.CommitID=$COMMIT \
        -X github.com/minio/mc/cmd.ShortCommitID=$(echo $COMMIT | cut -c1-12)" && \
    cp LICENSE CREDITS /out/

# same base as upstream's image (ubi9-micro: sh + coreutils, which storage-init's script needs)
FROM registry.access.redhat.com/ubi9/ubi-micro:9.6
ARG RELEASE
COPY --from=build /out/mc /usr/bin/mc
COPY --from=build /out/LICENSE /out/CREDITS /licenses/
LABEL org.opencontainers.image.source=https://github.com/minio/mc \
      org.opencontainers.image.version=$RELEASE \
      org.opencontainers.image.licenses=AGPL-3.0-only \
      org.opencontainers.image.description="MinIO client (mc) $RELEASE built from source for amd64 + arm64 (upstream stopped publishing)."
ENTRYPOINT ["mc"]
