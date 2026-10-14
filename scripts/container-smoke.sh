#!/bin/sh
# Exercise the real image entrypoints under the documented runtime restrictions.
set -eu
prefix=${IMAGE_PREFIX:-facts-engine}
tag=${IMAGE_TAG:-local}
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/facts-container-smoke.XXXXXXXX")
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM
printf '{}\n' > "$work_dir/config.json"
chmod 755 "$work_dir"
chmod 444 "$work_dir/config.json"
for binary in memoryd memoryworker; do
  image="$prefix/$binary:$tag"
  user=$(docker image inspect --format '{{.Config.User}}' "$image")
  [ "$user" = '65532:65532' ] || { printf 'Unexpected container user\n' >&2; exit 1; }
  status=0
  docker run --rm --network none --read-only --cap-drop ALL --security-opt no-new-privileges "$image" > "$work_dir/output" 2>&1 || status=$?
  [ "$status" -eq 2 ] || { cat "$work_dir/output" >&2; exit 1; }
  grep -q 'usage:' "$work_dir/output"
  status=0
  docker run --rm --network none --read-only --cap-drop ALL --security-opt no-new-privileges \
    --mount "type=bind,src=$work_dir/config.json,dst=/config.json,readonly" \
    "$image" -config /config.json > "$work_dir/output" 2>&1 || status=$?
  [ "$status" -eq 1 ] || { cat "$work_dir/output" >&2; exit 1; }
  grep -q 'configuration rejected' "$work_dir/output"
  printf '%s: non-root read-only entrypoint checks passed\n' "$binary"
done
