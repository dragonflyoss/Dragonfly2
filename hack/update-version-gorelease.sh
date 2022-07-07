#!/bin/sh

GO_VERSION=$(go version | grep -o 'go[^ ].*')

sed -i "s#\tGoVersion  = ".*"#\tGoVersion  = \"${GO_VERSION}\"#" version/version.go
