FROM ubuntu:latest

COPY ./eimer-kette /usr/local/bin/eimer-kette

CMD ["/usr/local/bin/eimer-kette", "version"]
