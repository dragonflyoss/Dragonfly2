#!/bin/bash

SRC="$(cd "$(dirname "$0")/.." && pwd)"

echo "work dir:$SRC"
if protoc -I="$SRC" \
  -I $GOPATH/pkg/mod/github.com/envoyproxy/protoc-gen-validate@v0.6.1 \
  --go_out "$SRC" --go_opt paths=source_relative \
  --go-grpc_out "$SRC" --go-grpc_opt paths=source_relative \
  --validate_out "lang=go,paths=source_relative:$SRC" \
  "$SRC"/internal/rpc/base/*.proto \
  "$SRC"/internal/rpc/cdnsystem/*.proto \
  "$SRC"/internal/rpc/dfdaemon/*.proto \
  "$SRC"/internal/rpc/scheduler/*.proto \
  "$SRC"/internal/rpc/manager/*.proto; then
  echo "generate grpc code success"
  if cd "$SRC" && go mod tidy; then
    echo "go mod tidy success"
  else
    echo "go mod tidy fail"
  fi
else
  echo "generate grpc code fail"
fi
