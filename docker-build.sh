#!/bin/sh

rm -rf ./bin/
docker run -it --rm \
	--name my-golang-project \
	-v "$PWD":/home \
 inrgihc/centos7-golang:1.14.12 /bin/sh -c 'mkdir -p ./bin && go mod download && go build -o ./bin/greenplum_exporter'

