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
PKG_LIST := $(shell go list ${PKG}/... | grep -v /vendor/ | grep -v '\(/cdnsystem/\|manager\)')
GIT_COMMIT := $(shell git rev-parse --verify HEAD --short=7)
GIT_COMMIT_LONG := $(shell git rev-parse --verify HEAD)
DFGET_ARCHIVE_PREFIX := "$(DFGET_NAME)_$(GIT_COMMIT)"

all: help

build-dirs: ## Prepare required folders for build
	@mkdir -p ./bin
.PHONY: build-dirs

docker-build: docker-build-cdn docker-build-dfdaemon docker-build-scheduler
	@echo "Build image done."
.PHONY: docker-build

docker-push: docker-push-cdn docker-push-dfdaemon docker-push-scheduler
	@echo "Push image done."
.PHONY: docker-push

docker-build-cdn: ## Build cdn image
	@echo "Begin to use docker build cdn image."
	./hack/docker-build.sh cdn
.PHONY: docker-build-cdn

docker-build-dfdaemon: ## Build dfdaemon image
	@echo "Begin to use docker build dfdaemon image."
	./hack/docker-build.sh dfdaemon
.PHONY: docker-build-dfdaemon

docker-build-scheduler: ## Build scheduler image
	@echo "Begin to use docker build scheduler image."
	./hack/docker-build.sh scheduler
.PHONY: docker-build-scheduler

docker-push-cdn: docker-build-cdn ## Push cdn image
	@echo "Begin to push cdn docker image."
	./hack/docker-push.sh cdn
.PHONY: docker-push-cdn

docker-push-dfdaemon: docker-build-dfdaemon ## Push dfdaemon image
	@echo "Begin to push dfdaemon docker image."
	./hack/docker-push.sh dfdaemon
.PHONY: docker-push-dfdaemon

docker-push-scheduler: docker-build-scheduler ## Push scheduler image
	@echo "Begin to push dfdaemon docker image."
	./hack/docker-push.sh scheduler
.PHONY: docker-push-scheduler

build: build-cdn build-scheduler build-dfget build-manager ## Build dragonfly
.PHONY: build

build-cdn: build-dirs ## Build cdn
	@echo "Begin to build cdn."
	./hack/build.sh cdn
.PHONY: build-cdn

build-dfget: build-dirs ## Build dfget
	@echo "Begin to build dfget."
	./hack/build.sh dfget
.PHONY: build-dfget

build-linux-dfget: build-dirs ## Build linux dfget
	@echo "Begin to build linux dfget."
	GOOS=linux GOARCH=amd64 ./hack/build.sh dfget
.PHONY: build-linux-dfget

build-scheduler: build-dirs ## Build scheduler
	@echo "Begin to build scheduler."
	./hack/build.sh scheduler
.PHONY: build-scheduler

build-manager: build-dirs ## Build manager
	@echo "Begin to build manager."
	./hack/build.sh manager
.PHONY: build-manager

install-cdn: ## Install cdn
	@echo "Begin to install cdn."
	./hack/install.sh install cdn
.PHONY: install-cdn

install-dfget: ## Install dfget
	@echo "Begin to install dfget."
	./hack/install.sh install dfget
.PHONY: install-dfget

install-scheduler: ## Install scheduler
	@echo "Begin to install scheduler."
	./hack/install.sh install scheduler
.PHONY: install-scheduler

install-manager: ## Install manager
	@echo "Begin to install manager."
	./hack/install.sh install manager
.PHONY: install-manager

# TODO more arch like arm, aarch64
build-rpm-dfget: build-linux-dfget
	@echo "Begin to build rpm dfget"
	@docker run --rm \
	-v "$(PWD)/build:/root/build" \
	-v "$(PWD)/docs:/root/docs" \
	-v "$(PWD)/License:/root/License" \
	-v "$(PWD)/CHANGELOG.md:/root/CHANGELOG.md" \
	-v "$(PWD)/bin:/root/bin" \
	-e "VERSION=$(GIT_VERSION)" \
	goreleaser/nfpm pkg \
		--config /root/build/package/nfpm/dfget.yaml \
		--target /root/bin/$(DFGET_ARCHIVE_PREFIX)_linux_amd64.rpm
.PHONY: build-rpm-dfget

build-deb-dfget: build-linux-dfget
	@echo "Begin to build deb dfget"
	@docker run --rm \
	-v "$(PWD)/build:/root/build" \
	-v "$(PWD)/docs:/root/docs" \
	-v "$(PWD)/License:/root/License" \
	-v "$(PWD)/CHANGELOG.md:/root/CHANGELOG.md" \
	-v "$(PWD)/bin:/root/bin" \
	-e "VERSION=$(GIT_VERSION)" \
	goreleaser/nfpm pkg \
		--config /root/build/package/nfpm/dfget.yaml \
		--target /root/bin/$(DFGET_ARCHIVE_PREFIX)_linux_amd64.deb
.PHONY: build-deb-dfget

test: ## Run unittests
	@go test -race -short ${PKG_LIST}
.PHONY: test

test-coverage: ## Run tests with coverage
	@go test -race -short ${PKG_LIST} -coverprofile cover.out -covermode=atomic
	@cat cover.out >> coverage.txt
.PHONY: test-coverage

swag-manager:
	@swag init -g cmd/manager/main.go -o api/v2/manager
.PHONY: swag-manager

changelog:
	@git-chglog -o CHANGELOG.md

clean:
	@go clean
	@rm -rf bin .go .cache

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
	@echo "make test                           run unittests"
	@echo "make test-coverage                  run tests with coverage"
	@echo "make swag-manager                   generate swagger api"
	@echo "make changelog                      generate CHANGELOG.md"
	@echo "make clean                          clean"
