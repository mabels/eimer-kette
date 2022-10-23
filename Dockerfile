FROM alpine

COPY eimer-kette /usr/bin/eimer-kette

ENTRYPOINT ["/usr/bin/eimer-kette"]
