#!/bin/bash

SRC="$(cd "$(dirname "$0")/.." && pwd)"

echo "work dir: $SRC"

if protoc -I="$SRC" \
  --go_out "$SRC" --go_opt paths=source_relative \
  --go-grpc_out "$SRC" --go-grpc_opt paths=source_relative \
  "$SRC"/pkg/grpc/base/*.proto \
  "$SRC"/pkg/grpc/cdnsystem/*.proto \
  "$SRC"/pkg/grpc/dfdaemon/*.proto \
  "$SRC"/pkg/grpc/scheduler/*.proto; then
  echo "generate grpc code successfully"
else
  echo "generate grpc code fail"
fi
cd "$SRC" && go mod tidy