.PHONY: clean build

VERSION=1.0.0

clean:
	rm -rf ./docker_build/hermes
	rm -rf ./docker_build/hermes.upx

build:
	env CGO_ENABLED=0 go build -ldflags="-s -w" -o docker_build/hermes . && upx --brute ./docker_build/hermes
	rm -rf ./docker_build/hermes.upx

build_dev:
	env CGO_ENABLED=0 go build -ldflags="-s -w" -o docker_build/hermes

docker: clean build
	docker build --no-cache  -t xuanloc0511/hermes:${VERSION} ./docker_build

docker_dev: clean build_dev
	docker build --no-cache  -t xuanloc0511/hermes:${VERSION} ./docker_build