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
DFCACHE_NAME := "dfcache"
DFSTORE_NAME := "dfstore"
SEMVER := "2.1.0"
VERSION_RELEASE := "1"
PKG := "$(PROJECT_NAME)"
PKG_LIST := $(shell go list ${PKG}/... | grep -v /vendor/ | grep -v '\(/test/\)')
GIT_COMMIT := $(shell git rev-parse --verify HEAD --short=7)
GIT_COMMIT_LONG := $(shell git rev-parse --verify HEAD)
DFGET_ARCHIVE_PREFIX := "$(DFGET_NAME)_$(SEMVER)-$(VERSION_RELEASE)_$(GIT_COMMIT)"
DFCACHE_ARCHIVE_PREFIX := "$(DFCACHE_NAME)_$(SEMVER)-$(VERSION_RELEASE)_$(GIT_COMMIT)"
DFSTORE_ARCHIVE_PREFIX := "$(DFSTORE_NAME)_$(SEMVER)-$(VERSION_RELEASE)_$(GIT_COMMIT)"

all: help

# Prepare required folders for build.
build-dirs:
	@mkdir -p ./bin
.PHONY: build-dirs

# Build dragonfly.
docker-build: docker-build-dfdaemon docker-build-scheduler docker-build-manager docker-build-trainer
	@echo "Build image done."
.PHONY: docker-build

# Push dragonfly images.
docker-push: docker-push-dfdaemon docker-push-scheduler docker-push-manager docker-build-trainer
	@echo "Push image done."
.PHONY: docker-push

# Build dfdaemon image.
docker-build-dfdaemon:
	@echo "Begin to use docker build dfdaemon image."
	./hack/docker-build.sh dfdaemon
.PHONY: docker-build-dfdaemon

# Build scheduler image.
docker-build-scheduler:
	@echo "Begin to use docker build scheduler image."
	./hack/docker-build.sh scheduler
.PHONY: docker-build-scheduler

# Build manager image.
docker-build-manager:
	@echo "Begin to use docker build manager image."
	./hack/docker-build.sh manager
.PHONY: docker-build-manager

# Build trainer image.
docker-build-trainer:
	@echo "Begin to use docker build trainer image."
	./hack/docker-build.sh trainer
.PHONY: docker-build-trainer

# Build testing tools image.
docker-build-testing-tools: build-dirs
	@echo "Begin to testing tools image."
	./test/tools/no-content-length/build.sh
.PHONY: docker-build-testing-tools

# Push dfdaemon image.
docker-push-dfdaemon: docker-build-dfdaemon
	@echo "Begin to push dfdaemon docker image."
	./hack/docker-push.sh dfdaemon
.PHONY: docker-push-dfdaemon

# Push scheduler image.
docker-push-scheduler: docker-build-scheduler
	@echo "Begin to push scheduler docker image."
	./hack/docker-push.sh scheduler
.PHONY: docker-push-scheduler

# Push manager image.
docker-push-manager: docker-build-manager
	@echo "Begin to push manager docker image."
	./hack/docker-push.sh manager
.PHONY: docker-push-manager

# Push trainer image.
docker-push-trainer: docker-build-trainer
	@echo "Begin to push trainer docker image."
	./hack/docker-push.sh trainer
.PHONY: docker-push-trainer

# Build dragonfly.
build: build-manager build-scheduler build-trainer build-dfget build-dfcache build-dfstore
.PHONY: build

# Build dfget.
build-dfget: build-dirs
	@echo "Begin to build dfget."
	./hack/build.sh dfget
.PHONY: build-dfget

# Build linux dfget.
build-linux-dfget: build-dirs
	@echo "Begin to build linux dfget."
	GOOS=linux GOARCH=amd64 ./hack/build.sh dfget
.PHONY: build-linux-dfget

# Build dfcache.
build-dfcache: build-dirs
	@echo "Begin to build dfcache."
	./hack/build.sh dfcache
.PHONY: build-dfcache

# Build linux dfcache.
build-linux-dfcache: build-dirs
	@echo "Begin to build linux dfcache."
	GOOS=linux GOARCH=amd64 ./hack/build.sh dfcache
.PHONY: build-linux-dfcache

# Build dfstore.
build-dfstore: build-dirs
	@echo "Begin to build dfstore."
	./hack/build.sh dfstore
.PHONY: build-dfstore

# Build linux dfcache.
build-linux-dfstore: build-dirs
	@echo "Begin to build linux dfstore."
	GOOS=linux GOARCH=amd64 ./hack/build.sh dfstore
.PHONY: build-linux-dfstore

# Build scheduler.
build-scheduler: build-dirs
	@echo "Begin to build scheduler."
	./hack/build.sh scheduler
.PHONY: build-scheduler

# Build manager.
build-manager: build-dirs build-manager-console
	@echo "Begin to build manager."
	make build-manager-server
.PHONY: build-manager

# Build manager server.
build-manager-server: build-dirs
	@echo "Begin to build manager server."
	./hack/build.sh manager
.PHONY: build-manager

# Build manager console.
build-manager-console: build-dirs
	@echo "Begin to build manager console."
	./hack/build.sh manager-console
.PHONY: build-manager-console

# Build trainer.
build-trainer: build-dirs
	@echo "Begin to build trainer."
	./hack/build.sh trainer
.PHONY: build-trainer

# Install dfget.
install-dfget:
	@echo "Begin to install dfget."
	./hack/install.sh install dfget
.PHONY: install-dfget

# Install scheduler.
install-scheduler:
	@echo "Begin to install scheduler."
	./hack/install.sh install scheduler
.PHONY: install-scheduler

# Install manager.
install-manager:
	@echo "Begin to install manager."
	./hack/install.sh install manager
.PHONY: install-manager

# Install trainer.
install-trainer:
	@echo "Begin to install trainer."
	./hack/install.sh install trainer 
.PHONY: install-trainer

# Build rpm dfget.
build-rpm-dfget: build-linux-dfget
	@echo "Begin to build rpm dfget."
	@docker run --rm \
	-v "$(PWD)/build:/root/build" \
	-v "$(PWD)/build/package/docs:/root/docs" \
	-v "$(PWD)/LICENSE:/root/License" \
	-v "$(PWD)/CHANGELOG.md:/root/CHANGELOG.md" \
	-v "$(PWD)/bin:/root/bin" \
	-e "SEMVER=$(SEMVER)" \
	-e "VERSION_RELEASE=$(VERSION_RELEASE)" \
	goreleaser/nfpm pkg \
		--config /root/build/package/nfpm/dfget.yaml \
		--target /root/bin/$(DFGET_ARCHIVE_PREFIX)_linux_amd64.rpm
	@echo "Build package output: ./bin/$(DFGET_ARCHIVE_PREFIX)_linux_amd64.rpm"
.PHONY: build-rpm-dfget

# Build rpm dfcache.
build-rpm-dfcache: build-linux-dfcache build-dfcache-man-page
	@echo "Begin to build rpm dfcache."
	@docker run --rm \
	-v "$(PWD)/build:/root/build" \
	-v "$(PWD)/build/package/docs:/root/docs" \
	-v "$(PWD)/LICENSE:/root/License" \
	-v "$(PWD)/CHANGELOG.md:/root/CHANGELOG.md" \
	-v "$(PWD)/bin:/root/bin" \
	-e "SEMVER=$(SEMVER)" \
	-e "VERSION_RELEASE=$(VERSION_RELEASE)" \
	goreleaser/nfpm pkg \
		--config /root/build/package/nfpm/dfcache.yaml \
		--target /root/bin/$(DFCACHE_ARCHIVE_PREFIX)_linux_amd64.rpm
	@echo "Build package output: ./bin/$(DFCACHE_ARCHIVE_PREFIX)_linux_amd64.rpm"
.PHONY: build-rpm-dfcache

# Build rpm dfstore.
build-rpm-dfstore: build-linux-dfstore
	@echo "Begin to build rpm dfstore."
	@docker run --rm \
	-v "$(PWD)/build:/root/build" \
	-v "$(PWD)/build/package/docs:/root/docs" \
	-v "$(PWD)/LICENSE:/root/License" \
	-v "$(PWD)/CHANGELOG.md:/root/CHANGELOG.md" \
	-v "$(PWD)/bin:/root/bin" \
	-e "SEMVER=$(SEMVER)" \
	-e "VERSION_RELEASE=$(VERSION_RELEASE)" \
	goreleaser/nfpm pkg \
		--config /root/build/package/nfpm/dfstore.yaml \
		--target /root/bin/$(DFSTORE_ARCHIVE_PREFIX)_linux_amd64.rpm
	@echo "Build package output: ./bin/$(DFSTORE_ARCHIVE_PREFIX)_linux_amd64.rpm"
.PHONY: build-rpm-dfstore

# Build deb dfget.
build-deb-dfget: build-linux-dfget
	@echo "Begin to build deb dfget."
	@docker run --rm \
	-v "$(PWD)/build:/root/build" \
	-v "$(PWD)/build/package/docs:/root/docs" \
	-v "$(PWD)/LICENSE:/root/License" \
	-v "$(PWD)/CHANGELOG.md:/root/CHANGELOG.md" \
	-v "$(PWD)/bin:/root/bin" \
	-e "SEMVER=$(SEMVER)" \
	-e "VERSION_RELEASE=$(VERSION_RELEASE)" \
	goreleaser/nfpm pkg \
		--config /root/build/package/nfpm/dfget.yaml \
		--target /root/bin/$(DFGET_ARCHIVE_PREFIX)_linux_amd64.deb
	@echo "Build package output: ./bin/$(DFGET_ARCHIVE_PREFIX)_linux_amd64.deb"
.PHONY: build-deb-dfget

# Build deb dfcache.
build-deb-dfcache: build-linux-dfcache build-dfcache-man-page
	@echo "Begin to build deb dfcache."
	@docker run --rm \
	-v "$(PWD)/build:/root/build" \
	-v "$(PWD)/build/package/docs:/root/docs" \
	-v "$(PWD)/LICENSE:/root/License" \
	-v "$(PWD)/CHANGELOG.md:/root/CHANGELOG.md" \
	-v "$(PWD)/bin:/root/bin" \
	-e "SEMVER=$(SEMVER)" \
	-e "VERSION_RELEASE=$(VERSION_RELEASE)" \
	goreleaser/nfpm pkg \
		--config /root/build/package/nfpm/dfcache.yaml \
		--target /root/bin/$(DFCACHE_ARCHIVE_PREFIX)_linux_amd64.deb
	@echo "Build package output: ./bin/$(DFCACHE_ARCHIVE_PREFIX)_linux_amd64.deb"
.PHONY: build-deb-dfcache

# Build deb dfstore
build-deb-dfstore: build-linux-dfstore
	@echo "Begin to build deb dfstore"
	@docker run --rm \
	-v "$(PWD)/build:/root/build" \
	-v "$(PWD)/build/package/docs:/root/docs" \
	-v "$(PWD)/LICENSE:/root/License" \
	-v "$(PWD)/CHANGELOG.md:/root/CHANGELOG.md" \
	-v "$(PWD)/bin:/root/bin" \
	-e "SEMVER=$(SEMVER)" \
	-e "VERSION_RELEASE=$(VERSION_RELEASE)" \
	goreleaser/nfpm pkg \
		--config /root/build/package/nfpm/dfstore.yaml \
		--target /root/bin/$(DFSTORE_ARCHIVE_PREFIX)_linux_amd64.deb
	@echo "Build package output: ./bin/$(DFSTORE_ARCHIVE_PREFIX)_linux_amd64.deb"
.PHONY: build-deb-dfstore

# Generate man page.
build-man-page: build-dfget-man-page build-dfcache-man-page build-dfstore-man-page
.PHONY: build-man-page

# Generate dfget man page.
build-dfget-man-page:
	@pandoc -s -t man ./build/package/docs/dfget.1.md -o ./build/package/docs/dfget.1
.PHONY: build-dfget-man-page

# Genrate dfcache man pages.
build-dfcache-man-page:
	@pandoc -s -t man ./build/package/docs/dfcache/dfcache.md -o ./build/package/docs/dfcache/dfcache.1
	@pandoc -s -t man ./build/package/docs/dfcache/dfcache_delete.md -o ./build/package/docs/dfcache/dfcache-delete.1
	@pandoc -s -t man ./build/package/docs/dfcache/dfcache_doc.md -o ./build/package/docs/dfcache/dfcache-doc.1
	@pandoc -s -t man ./build/package/docs/dfcache/dfcache_export.md -o ./build/package/docs/dfcache/dfcache-export.1
	@pandoc -s -t man ./build/package/docs/dfcache/dfcache_import.md -o ./build/package/docs/dfcache/dfcache-import.1
	@pandoc -s -t man ./build/package/docs/dfcache/dfcache_plugin.md -o ./build/package/docs/dfcache/dfcache-plugin.1
	@pandoc -s -t man ./build/package/docs/dfcache/dfcache_stat.md -o ./build/package/docs/dfcache/dfcache-stat.1
	@pandoc -s -t man ./build/package/docs/dfcache/dfcache_version.md -o ./build/package/docs/dfcache/dfcache-version.1
.PHONY: build-dfcache-man-page

# Genrate dfstore man pages.
build-dfstore-man-page:
	@pandoc -s -t man ./build/package/docs/dfstore/dfstore.md -o ./build/package/docs/dfstore/dfstore.1
	@pandoc -s -t man ./build/package/docs/dfstore/dfstore_copy.md -o ./build/package/docs/dfstore/dfstore-copy.1
	@pandoc -s -t man ./build/package/docs/dfstore/dfstore_remove.md -o ./build/package/docs/dfstore/dfstore-remove.1
	@pandoc -s -t man ./build/package/docs/dfstore/dfstore_version.md -o ./build/package/docs/dfstore/dfstore-version.1
.PHONY: build-dfstore-man-page

# Generate e2e sha256sum.
build-e2e-sha256sum:
	@GOOS=linux GOARCH=amd64 go build -o /tmp/sha256sum-offset test/tools/sha256sum-offset/main.go
.PHONY: build-e2e-sha256sum

# Generate e2e download grpc test binary.
build-e2e-download-grpc-test:
	@GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/download-grpc-test test/tools/download-grpc-test/main.go
.PHONY: build-e2e-download-grpc-test

# Run unittests.
test:
	@go test -v -race -short ${PKG_LIST}
.PHONY: test

# Run tests with coverage.
test-coverage:
	@go test -v -race -short ${PKG_LIST} -coverprofile cover.out -covermode=atomic
	@cat cover.out >> coverage.txt
.PHONY: test-coverage

# Run github actions E2E tests with coverage.
actions-e2e-test-coverage:
	@ginkgo -v -r --race --fail-fast --cover --trace --show-node-events test/e2e
	@cat coverprofile.out >> coverage.txt
.PHONY: actions-e2e-test-coverage

# Install E2E tests environment.
install-e2e-test:
	@./hack/install-e2e-test.sh
.PHONY: install-e2e-test

# Run E2E tests.
e2e-test: install-e2e-test build-e2e-sha256sum build-e2e-download-grpc-test
	@ginkgo -v -r --race --fail-fast --cover --trace --show-node-events test/e2e
.PHONY: e2e-test

# Run E2E tests with coverage.
e2e-test-coverage: install-e2e-test build-e2e-sha256sum build-e2e-download-grpc-test
	@ginkgo -v -r --race --fail-fast --cover --trace --show-node-events test/e2e
	@cat coverprofile.out >> coverage.txt
.PHONY: e2e-test-coverage

# Clean E2E tests.
clean-e2e-test: 
	@kind delete cluster
	@echo "cleaning log file."
	@rm -rf test/e2e/*.log
.PHONY: clean-e2e-test

# Kind load dragonfly.
kind-load: kind-load-scheduler kind-load-dfdaemon kind-load-manager kind-load-trainer kind-load-testing-tools
	@echo "Kind load image done."
.PHONY: kind-load

# Run kind load docker scheduler.
kind-load-scheduler:
	@./hack/kind-load.sh scheduler
.PHONY: kind-load-scheduler

# Run kind load docker dfget.
kind-load-dfdaemon:
	@./hack/kind-load.sh dfdaemon
.PHONY: kind-load-dfget

# Run kind load docker manager.
kind-load-manager:
	@./hack/kind-load.sh manager
.PHONY: kind-load-manager

# Run kind load docker trainer.
kind-load-trainer:
	@./hack/kind-load.sh trainer
.PHONY: kind-load-trainer

# Run kind load docker testing tools.
kind-load-testing-tools:
	@./hack/kind-load.sh no-content-length
.PHONY: kind-load-testing-tools

# Run code lint.
lint: markdownlint
	@echo "Begin to golangci-lint."
	@golangci-lint run
.PHONY: lint

# Run markdown lint.
markdownlint:
	@echo "Begin to markdownlint."
	@./hack/markdownlint.sh
.PHONY: markdownlint

# Run go generate.
generate:
	@go generate ${PKG_LIST}
.PHONY: generate

# Generate swagger files.
swag:
	@swag init --parseDependency --parseInternal -g cmd/manager/main.go -o api/manager

# Generate changelog.
changelog:
	@git-chglog -o CHANGELOG.md
.PHONY: changelog

# Clear compiled files.
clean:
	@go clean
	@rm -rf bin .go .cache
.PHONY: clean

help: 
	@echo "make build-dirs                     prepare required folders for build"
	@echo "make docker-build                   build dragonfly image"
	@echo "make docker-push                    push dragonfly image"
	@echo "make docker-build-dfdaemon          build dfdaemon image"
	@echo "make docker-build-scheduler         build scheduler image"
	@echo "make docker-build-manager           build manager image"
	@echo "make docker-build-trainer           build trainer image"
	@echo "make docker-push-dfdaemon           push dfdaemon image"
	@echo "make docker-push-scheduler          push scheduler image"
	@echo "make docker-push-manager            push manager image"
	@echo "make docker-push-trainer            push trainer image"
	@echo "make build                          build dragonfly"
	@echo "make build-dfget                    build dfget"
	@echo "make build-linux-dfget              build linux dfget"
	@echo "make build-dfcache                  build dfcache"
	@echo "make build-linux-dfcache            build linux dfcache"
	@echo "make build-dfstore                  build dfstore"
	@echo "make build-linux-dfstore            build linux dfstore"
	@echo "make build-scheduler                build scheduler"
	@echo "make build-manager                  build manager"
	@echo "make build-manager-server           build manager server"
	@echo "make build-manager-console          build manager console"
	@echo "make build-trainer                  build trainer"
	@echo "make build-e2e-sha256sum            build sha256sum test tool"
	@echo "make build-e2e-download-grpc-test   build download grpc test tool"
	@echo "make install-dfget                  install dfget"
	@echo "make install-scheduler              install scheduler"
	@echo "make install-manager                install manager"
	@echo "make install-trainer                install trainer"
	@echo "make build-rpm-dfget                build rpm dfget"
	@echo "make build-rpm-dfcache              build rpm dfcache"
	@echo "make build-rpm-dfstore              build rpm dfstore"
	@echo "make build-deb-dfget                build deb dfget"
	@echo "make build-deb-dfcache              build deb dfcache"
	@echo "make build-deb-dfstore              build deb dfstore"
	@echo "make build-man-page                 generate man page"
	@echo "make build-dfget-man-page           generate dfget man page"
	@echo "make build-dfcache-man-page         generate dfcache man page"
	@echo "make build-dfstore-man-page         generate dfstore man page"
	@echo "make test                           run unit tests"
	@echo "make test-coverage                  run tests with coverage"
	@echo "make actions-e2e-test-coverage      run github actons E2E tests with coverage"
	@echo "make install-e2e-test               install E2E tests environment"
	@echo "make e2e-test                       run e2e tests"
	@echo "make e2e-test-coverage              run e2e tests with coverage"
	@echo "make clean-e2e-test                 clean e2e tests"
	@echo "make kind-load                      kind load docker image"
	@echo "make kind-load-scheduler            kind load scheduler docker image"
	@echo "make kind-load-dfdaemon             kind load dfdaemon docker image"
	@echo "make kind-load-manager              kind load manager docker image"
	@echo "make kind-load-trainer              kind load trainer docker image"
	@echo "make kind-load-testing-tools        kind load testing tools docker image"
	@echo "make lint                           run code lint"
	@echo "make markdownlint                   run markdown lint"
	@echo "make generate                       run go generate"
	@echo "make swag                           generate swagger api docs"
	@echo "make changelog                      generate CHANGELOG.md"
	@echo "make clean                          clean"
