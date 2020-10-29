ARG ARCH="amd64"
ARG OS="linux"
FROM quay.io/prometheus/busybox-${OS}-${ARCH}:glibc

COPY ./bin/greenplum_exporter /bin/greenplum_exporter

EXPOSE      9297
USER        root
ENTRYPOINT  [ "/bin/greenplum_exporter" ]
