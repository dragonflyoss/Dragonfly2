#!/bin/bash

SRC="$(cd "$(dirname "$0")/.." && pwd)"

<<<<<<< HEAD
echo "work dir: $SRC"

protoc -I="$SRC" --go_out=plugins=grpc,paths=source_relative:"$SRC" \
  "$SRC"/pkg/grpc/base/*.proto \
  "$SRC"/pkg/grpc/cdnsystem/*.proto \
  "$SRC"/pkg/grpc/dfdaemon/*.proto \
  "$SRC"/pkg/grpc/scheduler/*.proto

cd "$SRC" && go mod tidy

echo "generate grpc code successfully"
=======
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
>>>>>>> main
