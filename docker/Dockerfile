FROM golang:1.13.5-alpine AS builder

# Copy the code from the host and compile it
WORKDIR $GOPATH/src/github.com/timescale/tsbs
COPY . ./
RUN apk add --no-cache git make bash
RUN make all

FROM golang:1.13.5-alpine
# install bash shell
RUN apk add --update bash && rm -rf /var/cache/apk/*

ENV PATH ./:$PATH
COPY --from=builder $GOPATH/src/github.com/timescale/tsbs/bin/tsbs_* ./
COPY ./docker/docker_entrypoint.sh ./
RUN chmod 751 docker_entrypoint.sh
ENTRYPOINT ["./docker_entrypoint.sh"]