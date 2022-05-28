FROM ubuntu:latest

RUN find . -print

COPY ./eimer-kette-linux /usr/local/bin/eimer-kette

CMD ["/usr/local/bin/eimer-kette", "version"]
