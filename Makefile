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
PKG := "$(PROJECT_NAME)"
PKG_LIST := $(shell go list ${PKG}/... | grep -v /vendor/)
VERSION := 2.0.0

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
.PHONY: build-cdn

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
build-rpm-dfget:
	@ docker build \
		-t dfget-rpm-builder \
		-f hack/packaging/rpm/Dockerfile \
		.
	@ docker run --rm \
		-v ${PWD}/bin/rpm/amd64:/root/rpmbuild/RPMS/x86_64 dfget-rpm-builder \
		rpmbuild -bb --define "_dfget_version $(VERSION)" /root/rpmbuild/SPECS/dfget.spec
.PHONY: build-rpm-dfget

build-deb-dfget:
	@ docker build \
		-t dfget-deb-builder \
		-f hack/packaging/deb/Dockerfile \
		.
	@ docker run --rm \
		-v ${PWD}/bin/deb/amd64:/pkg dfget-deb-builder
.PHONY: build-deb-dfget

test: ## Run unittests
	@go test -race -short ${PKG_LIST}
.PHONY: test

test-coverage: ## Run tests with coverage
	@go test -race -short -coverprofile cover.out -covermode=atomic ./pkg/dynconfig/...
	@cat cover.out >> coverage.txt
.PHONY: test-coverage
