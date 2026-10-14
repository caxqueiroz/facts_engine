#!/bin/sh
# Build minimal runtime images without sending the repository or secrets to Docker.
set -eu
cd "$(dirname "$0")/.."
architecture=${CONTAINER_ARCH:-$(go env GOARCH)}
case "$architecture" in
  amd64|arm64) ;;
  *) printf 'CONTAINER_ARCH must be amd64 or arm64\n' >&2; exit 1 ;;
esac
prefix=${IMAGE_PREFIX:-facts-engine}
tag=${IMAGE_TAG:-local}
context_dir=$(mktemp -d "${TMPDIR:-/tmp}/facts-container.XXXXXXXX")
trap 'rm -rf "$context_dir"' EXIT HUP INT TERM
for binary in memoryd memoryworker; do
  CGO_ENABLED=0 GOOS=linux GOARCH="$architecture" go build -trimpath -o "$context_dir/$binary" "./cmd/$binary"
done
cp deploy/container/Dockerfile "$context_dir/Dockerfile"
for binary in memoryd memoryworker; do
  docker build --platform "linux/$architecture" --target "$binary" --tag "$prefix/$binary:$tag" "$context_dir"
done
