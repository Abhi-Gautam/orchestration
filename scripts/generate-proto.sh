#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tools="$root/tmp/tools"
mkdir -p "$tools"

GOBIN="$tools" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11

include=/usr/local/include
if command -v brew >/dev/null 2>&1; then
  include=$(brew --prefix protobuf)/include
elif [ -d /usr/include/google/protobuf ]; then
  include=/usr/include
fi

protoc \
  -I "$root/api" \
  -I "$include" \
  --plugin="protoc-gen-go=$tools/protoc-gen-go" \
  --go_out="$root" \
  --go_opt=module=orchestration \
  "$root/api/orchestration/v1/workflows.proto"

gofmt -w "$root/gen"
