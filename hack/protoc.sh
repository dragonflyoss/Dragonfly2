#!/bin/bash

SRC="$(cd "$(dirname "$0")/.." && pwd)"

echo "work dir:$SRC"
if protoc -I="$SRC" \
#  -I $GOPATH/pkg/mod/github.com/envoyproxy/protoc-gen-validate@v0.6.1 \
  -I /Users/sunweipeng1/go/bin/pkg/mod/github.com/mwitkow/go-proto-validators@v0.3.2 \
  --go_out "$SRC" --go_opt paths=source_relative \
  --go-grpc_out "$SRC" --go-grpc_opt paths=source_relative \
  --validate_out "lang=go,paths=source_relative:$SRC" \
#  "$SRC"/pkg/rpc/base/*.proto \
  "$SRC"/pkg/rpc/cdnsystem/*.proto; then
#  "$SRC"/pkg/rpc/dfdaemon/*.proto \
#  "$SRC"/pkg/rpc/scheduler/*.proto \
#  "$SRC"/pkg/rpc/manager/*.proto; then
  echo "generate grpc code success"
  # warning messages about symlinks, fix in golang 1.17
  # https://github.com/golang/go/issues/35941
  if cd "$SRC" && go mod tidy; then
    echo "go mod tidy success"
  else
    echo "go mod tidy fail"
  fi
else
  echo "generate grpc code fail"
fi
