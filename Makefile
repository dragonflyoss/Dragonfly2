# Copyright The Dragonfly Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

PROJECT_NAME := "d7y.io/dragonfly/v2"
DFGET_NAME := "dfget"
VERSION := "2.0.0"
PKG := "$(PROJECT_NAME)"
PKG_LIST := $(shell go list ${PKG}/... | grep -v /vendor/ | grep -v '\(/test/\)')
GIT_COMMIT := $(shell git rev-parse --verify HEAD --short=7)
GIT_COMMIT_LONG := $(shell git rev-parse --verify HEAD)
DFGET_ARCHIVE_PREFIX := "$(DFGET_NAME)_$(GIT_COMMIT)"

all: help

# Prepare required folders for build
build-dirs:
	@mkdir -p ./bin
.PHONY: build-dirs

# Build dragonlfy
docker-build: docker-build-cdn docker-build-dfdaemon docker-build-scheduler docker-build-manager
	@echo "Build image done."
.PHONY: docker-build

# Push dragonfly images
docker-push: docker-push-cdn docker-push-dfdaemon docker-push-scheduler docker-push-manager
	@echo "Push image done."
.PHONY: docker-push

# Build cdn image
docker-build-cdn:
	@echo "Begin to use docker build cdn image."
	./hack/docker-build.sh cdn
.PHONY: docker-build-cdn

# Build dfdaemon image
docker-build-dfdaemon:
	@echo "Begin to use docker build dfdaemon image."
	./hack/docker-build.sh dfdaemon
.PHONY: docker-build-dfdaemon

# Build scheduler image
docker-build-scheduler:
	@echo "Begin to use docker build scheduler image."
	./hack/docker-build.sh scheduler
.PHONY: docker-build-scheduler

# Build manager image
docker-build-manager:
	@echo "Begin to use docker build manager image."
	./hack/docker-build.sh manager
.PHONY: docker-build-manager

# Build testing tools image
docker-build-testing-tools: build-dirs
	@echo "Begin to testing tools image."
	./test/tools/no-content-length/build.sh
.PHONY: docker-build-testing-tools

# Push cdn image
docker-push-cdn: docker-build-cdn
	@echo "Begin to push cdn docker image."
	./hack/docker-push.sh cdn
.PHONY: docker-push-cdn

# Push dfdaemon image
docker-push-dfdaemon: docker-build-dfdaemon
	@echo "Begin to push dfdaemon docker image."
	./hack/docker-push.sh dfdaemon
.PHONY: docker-push-dfdaemon

# Push scheduler image
docker-push-scheduler: docker-build-scheduler
	@echo "Begin to push dfdaemon docker image."
	./hack/docker-push.sh scheduler
.PHONY: docker-push-scheduler

# Push manager image
docker-push-manager: docker-build-manager
	@echo "Begin to push manager docker image."
	./hack/docker-push.sh manager
.PHONY: docker-push-manager

# Build dragonfly
build: build-cdn build-scheduler build-dfget build-manager
.PHONY: build

# Build cdn
build-cdn: build-dirs
	@echo "Begin to build cdn."
	./hack/build.sh cdn
.PHONY: build-cdn

# Build dfget
build-dfget: build-dirs
	@echo "Begin to build dfget."
	./hack/build.sh dfget
.PHONY: build-dfget

# Build linux dfget
build-linux-dfget: build-dirs
	@echo "Begin to build linux dfget."
	GOOS=linux GOARCH=amd64 ./hack/build.sh dfget
.PHONY: build-linux-dfget

# Build scheduler
build-scheduler: build-dirs
	@echo "Begin to build scheduler."
	./hack/build.sh scheduler
.PHONY: build-scheduler

# Build manager
build-manager: build-dirs
	@echo "Begin to build manager."
	./hack/build.sh manager
.PHONY: build-manager

# Install cdn
install-cdn:
	@echo "Begin to install cdn."
	./hack/install.sh install cdn
.PHONY: install-cdn

# Install dfget
install-dfget:
	@echo "Begin to install dfget."
	./hack/install.sh install dfget
.PHONY: install-dfget

# Install scheduler
install-scheduler:
	@echo "Begin to install scheduler."
	./hack/install.sh install scheduler
.PHONY: install-scheduler

# Install manager
install-manager:
	@echo "Begin to install manager."
	./hack/install.sh install manager
.PHONY: install-manager

# Build rpm dfget
build-rpm-dfget: build-linux-dfget
	@echo "Begin to build rpm dfget"
	@docker run --rm \
	-v "$(PWD)/build:/root/build" \
	-v "$(PWD)/docs:/root/docs" \
	-v "$(PWD)/LICENSE:/root/License" \
	-v "$(PWD)/CHANGELOG.md:/root/CHANGELOG.md" \
	-v "$(PWD)/bin:/root/bin" \
	-e "VERSION=$(GIT_VERSION)" \
	goreleaser/nfpm pkg \
		--config /root/build/package/nfpm/dfget.yaml \
		--target /root/bin/$(DFGET_ARCHIVE_PREFIX)_linux_amd64.rpm
.PHONY: build-rpm-dfget

# Build deb dfget
build-deb-dfget: build-linux-dfget
	@echo "Begin to build deb dfget"
	@docker run --rm \
	-v "$(PWD)/build:/root/build" \
	-v "$(PWD)/docs:/root/docs" \
	-v "$(PWD)/LICENSE:/root/License" \
	-v "$(PWD)/CHANGELOG.md:/root/CHANGELOG.md" \
	-v "$(PWD)/bin:/root/bin" \
	-e "VERSION=$(GIT_VERSION)" \
	goreleaser/nfpm pkg \
		--config /root/build/package/nfpm/dfget.yaml \
		--target /root/bin/$(DFGET_ARCHIVE_PREFIX)_linux_amd64.deb
.PHONY: build-deb-dfget

# Generate dfget man page
build-dfget-man-page:
	@pandoc -s -t man ./docs/en/cli-reference/dfget.1.md -o ./docs/en/cli-reference/dfget.1
.PHONY: build-man-page

# Run unittests
test:
	@go test -v -gcflags "all=-l" -race -short ${PKG_LIST}
.PHONY: test

# Run tests with coverage
test-coverage:
	@go test -v -gcflags "all=-l" -race -short ${PKG_LIST} -coverprofile cover.out -covermode=atomic
	@cat cover.out >> coverage.txt
.PHONY: test-coverage

# Run github actions E2E tests with coverage
actions-e2e-test-coverage:
	@ginkgo -v -r --race --failFast -cover test/e2e --trace --progress
	@cat test/e2e/*.coverprofile >> coverage.txt
.PHONY: actions-e2e-test-coverage

# Install E2E tests environment
install-e2e-test:
	@./hack/install-e2e-test.sh
.PHONY: install-e2e-test

# Run E2E tests
e2e-test: install-e2e-test
	@ginkgo -v -r --race --failFast test/e2e --trace --progress
.PHONY: e2e-test

# Run E2E tests with coverage
e2e-test-coverage: install-e2e-test
	@ginkgo -v -r --race --failFast -cover test/e2e --trace --progress
	@cat test/e2e/*.coverprofile >> coverage.txt
.PHONY: e2e-test-coverage

# Clean E2E tests
clean-e2e-test: 
	@kind delete cluster
.PHONY: clean-e2e-test

# Kind load dragonlfy
kind-load: kind-load-cdn kind-load-scheduler kind-load-dfdaemon kind-load-manager kind-load-testing-tools
	@echo "Kind load image done."
.PHONY: kind-load

# Run kind load docker-image cdn
kind-load-cdn:
	@./hack/kind-load.sh cdn
.PHONY: kind-load-cdn

# Run kind load docker scheduler
kind-load-scheduler:
	@./hack/kind-load.sh scheduler
.PHONY: kind-load-scheduler

# Run kind load docker dfget
kind-load-dfdaemon:
	@./hack/kind-load.sh dfdaemon
.PHONY: kind-load-dfget

# Run kind load docker manager
kind-load-manager:
	@./hack/kind-load.sh manager
.PHONY: kind-load-manager

# Run kind load docker testing tools
kind-load-testing-tools:
	@./hack/kind-load.sh no-content-length
.PHONY: kind-load-testing-tools

# Run code lint
lint: markdownlint
	@echo "Begin to golangci-lint."
	@golangci-lint run
.PHONY: lint

# Run markdown lint
markdownlint:
	@echo "Begin to markdownlint."
	@./hack/markdownlint.sh
.PHONY: markdownlint

# Run go generate
generate:
	@go generate ${PKG_LIST}
.PHONY: generate

swag:
	@swag init --parseDependency --parseInternal -g cmd/manager/main.go -o api/manager

# Generate changelog
changelog:
	@git-chglog -o CHANGELOG.md
.PHONY: changelog

clean:
	@go clean
	@rm -rf bin .go .cache
.PHONY: clean

help: 
	@echo "make build-dirs                     prepare required folders for build"
	@echo "make docker-build                   build dragonfly image"
	@echo "make docker-push                    push dragonfly image"
	@echo "make docker-build-cdn               build CDN image"
	@echo "make docker-build-dfdaemon          build dfdaemon image"
	@echo "make docker-build-scheduler         build scheduler image"
	@echo "make docker-push-cdn                push CDN image"
	@echo "make docker-push-dfdaemon           push dfdaemon image"
	@echo "make docker-push-scheduler          push scheduler image"
	@echo "make build                          build dragonfly"
	@echo "make build-cdn                      build CDN"
	@echo "make build-dfget                    build dfget"
	@echo "make build-dfget-linux              build linux dfget"
	@echo "make build-scheduler                build scheduler"
	@echo "make build-manager                  build manager"
	@echo "make install-cdn                    install CDN"
	@echo "make install-dfget                  install dfget"
	@echo "make install-scheduler              install scheduler"
	@echo "make install-manager                install manager"
	@echo "make build-rpm-dfget                build rpm dfget"
	@echo "make build-deb-dfget                build deb dfget"
	@echo "make build-dfget-man-page           generate dfget man page"
	@echo "make test                           run unit tests"
	@echo "make test-coverage                  run tests with coverage"
	@echo "make actions-e2e-test-coverage      run github actons E2E tests with coverage"
	@echo "make install-e2e-test               install E2E tests environment"
	@echo "make e2e-test                       run e2e tests"
	@echo "make e2e-test-coverage              run e2e tests with coverage"
	@echo "make clean-e2e-test                 clean e2e tests"
	@echo "make kind-load                      kind load docker image"
	@echo "make kind-load-cdn                  kind load cdn docker image"
	@echo "make kind-load-scheduler            kind load scheduler docker image"
	@echo "make kind-load-dfdaemon             kind load dfdaemon docker image"
	@echo "make kind-load-manager              kind load manager docker image"
	@echo "make kind-load-testing-tools        kind load testing tools docker image"
	@echo "make lint                           run code lint"
	@echo "make markdownlint                   run markdown lint"
	@echo "make swag                           generate swagger api docs"
	@echo "make changelog                      generate CHANGELOG.md"
	@echo "make generate                       run go generate"
	@echo "make clean                          clean"
