#!/bin/bash

SRC="$(cd "$(dirname "$0")/.." && pwd)"

echo "work dir: $SRC"

protoc -I="$SRC" --go_out=plugins=grpc,paths=source_relative:"$SRC" \
  "$SRC"/pkg/grpc/base/*.proto \
  "$SRC"/pkg/grpc/cdnsystem/*.proto \
  "$SRC"/pkg/grpc/dfdaemon/*.proto \
  "$SRC"/pkg/grpc/scheduler/*.proto

cd "$SRC" && go mod tidy

echo "generate grpc code successfully"
