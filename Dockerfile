#alpine build
FROM golang:1.16.2-alpine AS builder

RUN go env -w GO111MODULE=on && go env -w GOPROXY=https://goproxy.cn,https://goproxy.io,direct

WORKDIR /home

ADD . .

RUN mkdir bin && go mod download && go build -o ./bin/greenplum_exporter

# image
FROM alpine:latest

COPY --from=builder /home/bin/greenplum_exporter /home/greenplum_exporter

EXPOSE 9297

USER root

CMD  [ "/home/greenplum_exporter" , "--log.level=error"]
