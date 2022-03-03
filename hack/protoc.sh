#!/bin/bash

SRC="$(cd "$(dirname "$0")/.." && pwd)"

echo "work dir:$SRC"
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.27.1
go install github.com/envoyproxy/protoc-gen-validate@v0.6.6
if protoc -I="$SRC" \
  -I $GOPATH/pkg/mod/github.com/envoyproxy/protoc-gen-validate@v0.6.6 \
  --go_out "$SRC" --go_opt paths=source_relative \
  --go-grpc_out "$SRC" --go-grpc_opt paths=source_relative \
  --validate_out "lang=go,paths=source_relative:$SRC" \
  --plugin=protoc-gen-go=$GOPATH/bin/protoc-gen-go \
  --plugin=protoc-gen-validate=$GOPATH/bin/protoc-gen-validate \
  "$SRC"/pkg/rpc/base/*.proto \
  "$SRC"/pkg/rpc/cdnsystem/*.proto \
  "$SRC"/pkg/rpc/dfdaemon/*.proto \
  "$SRC"/pkg/rpc/scheduler/*.proto \
  "$SRC"/pkg/rpc/manager/*.proto; then
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
