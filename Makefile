docker-build-cdn:  ## Build cdn image
	@echo "Begin to use docker build cdn image."
	./hack/docker-build.sh cdn
.PHONY: docker-build-cdn

build-cdn: build-dirs  ## Build cdn
	@echo "Begin to build cdn."
	./hack/build.sh cdn
.PHONY: build-cdn

install-cdn:  ## Install cdn
	@echo "Begin to install cdn."
	./hack/install.sh install cdn
.PHONY: install-cdn