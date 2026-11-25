FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/openstack-migrate-exporter .

FROM gcr.io/distroless/static-debian12:nonroot
LABEL org.opencontainers.image.source="https://github.com/ryzwan29/openstack-migrate-exporter" \
      org.opencontainers.image.description="Prometheus exporter for OpenStack Nova live migrations"
COPY --from=build /out/openstack-migrate-exporter /openstack-migrate-exporter
USER nonroot
EXPOSE 9181
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s CMD ["/openstack-migrate-exporter", "-healthcheck"]
ENTRYPOINT ["/openstack-migrate-exporter"]
