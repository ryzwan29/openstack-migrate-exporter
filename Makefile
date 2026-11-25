IMAGE   ?= ghcr.io/ryzwan29/openstack-migrate-exporter
VERSION ?= v1.1.0

.PHONY: test build docker push
test:
	go vet ./... && go test -race ./...
build:
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o openstack-migrate-exporter .
docker:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) -t $(IMAGE):latest .
# docker login ghcr.io -u ryzwan29  (password = PAT with write:packages)
push: docker
	docker push $(IMAGE):$(VERSION)
	docker push $(IMAGE):latest