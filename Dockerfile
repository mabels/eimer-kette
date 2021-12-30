FROM golang:1.17-alpine

RUN apk add make gcc git libc-dev

RUN wget https://github.com/goreleaser/goreleaser/releases/download/v1.2.3/goreleaser_Linux_x86_64.tar.gz \
 && tar -C /usr/local/bin -xzvf goreleaser_Linux_x86_64.tar.gz goreleaser \
 && rm goreleaser_Linux_x86_64.tar.gz

COPY . /build
RUN cd /build && make build

FROM alpine:latest

COPY --from=0 /build/dist/eimer-kette_linux_amd64/eimer-kette /usr/local/bin/eimer-kette

CMD ["/usr/local/bin/eimer-kette"] 
