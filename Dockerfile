ARG GO_VERSION=1.13.1
FROM golang:${GO_VERSION}-alpine

ARG TSBS_VERSION=master
RUN apk update && apk add --no-cache --virtual .build-deps git \
    && mkdir -p ${GOPATH}/src/github.com/timescale/ \
    && cd ${GOPATH}/src/github.com/timescale/ \
    && git clone --depth=1 --branch ${TSBS_VERSION} https://github.com/timescale/tsbs.git \
    && cd $GOPATH/src/github.com/timescale/tsbs \
    && go get -d -v ./... \
    && go build -o /go/bin/ ./... \
    && rm -rf $GOPATH/src/github.com/timescale/tsbs \
    && apk del .build-deps
