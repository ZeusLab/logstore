.PHONY: clean build

VERSION=1.0.0

clean:
	rm -rf ./docker_build/hermes
	rm -rf ./docker_build/hermes.upx

build:
	env CGO_ENABLED=0 go build -ldflags="-s -w" -o docker/hermes

release:
	env CGO_ENABLED=0 go build -ldflags="-s -w" -o docker/hermes . && upx --brute ./docker/hermes
	rm -rf ./docker_build/hermes.upx

docker: clean build
	docker build --no-cache  -t xuanloc0511/hermes:${VERSION} ./docker

docker_release: clean release
	docker build --no-cache  -t xuanloc0511/hermes:${VERSION} ./docker