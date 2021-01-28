#!/bin/bash

SRC="$(cd "$(dirname "$0")/.." && pwd)"

echo "work dir:$SRC"

if protoc -I="$SRC" \
  --go_out "$SRC" --go_opt paths=source_relative \
  --go-grpc_out "$SRC" --go-grpc_opt paths=source_relative \
  "$SRC"/pkg/rpc/base/*.proto \
  "$SRC"/pkg/rpc/cdnsystem/*.proto \
  "$SRC"/pkg/rpc/dfdaemon/*.proto \
  "$SRC"/pkg/rpc/scheduler/*.proto \
  "$SRC"/pkg/rpc/manager/*.proto; then
  echo "generate grpc code success"
  if cd "$SRC" && go mod tidy; then
    echo "go mod tidy success"
  else
    echo "go mod tidy fail"
  fi
else
  echo "generate grpc code fail"
fi
