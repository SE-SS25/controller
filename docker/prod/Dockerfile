FROM golang:1.24

WORKDIR /root
COPY ../../src ./src
COPY ../../go.sum .
COPY ../../go.mod .

ENV APP_ENV=prod

RUN go mod tidy
RUN go mod download

RUN go build -o /bin/controller ./src


FROM debian:bookworm-slim

COPY --from=0 /bin/controller /bin/controller

CMD ["/bin/controller"]