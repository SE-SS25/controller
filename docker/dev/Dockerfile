FROM golang:1.24

WORKDIR /root
COPY ./src ./src
COPY ./go.sum .
COPY ./go.mod .

COPY ./gomigrate ./gomigrate

ENV APP_ENV=dev

RUN go mod tidy
RUN go mod download

RUN go build -o /bin/controller ./src


FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY ./migrations ./migrations
COPY --from=0 /bin/controller /bin/controller

CMD ["/bin/controller"]