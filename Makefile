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

build-dirs: ## Prepare required folders for build
	@mkdir -p ./bin
.PHONY: build-dirs

docker-build-cdn:  ## Build cdn image
	@echo "Begin to use docker build cdn image."
	./hack/docker-build.sh cdn
.PHONY: docker-build-cdn

build-cdn: build-dirs  ## Build cdn
	@echo "Begin to build cdn."
	./hack/build.sh cdn
.PHONY: build-cdn

build-dfget: build-dirs  ## Build cdn
	@echo "Begin to build cdn."
	./hack/build.sh dfget
.PHONY: build-cdn

install-cdn:  ## Install cdn
	@echo "Begin to install cdn."
	./hack/install.sh install cdn
.PHONY: install-cdn