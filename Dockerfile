#lpine build
FROM golang:1.14-alpine AS builder
ENV GOBIN=$GOPATH/bin
ENV GO111MODULE=on
ENV GOPROXY=https://goproxy.io,direct
ENV TZ=Asia/Shanghai
WORKDIR /home
ADD . .
RUN mkdir bin && go mod download && go build -o ./bin/greenplum_exporter

# image
FROM alpine:latest
COPY --from=builder /home/bin/greenplum_exporter /home/greenplum_exporter
EXPOSE      9297
USER        root
CMD  [ "/home/greenplum_exporter" , "--log.level=error"]
