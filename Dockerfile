FROM golang:1.16-alpine as builder
RUN apk add --no-cache git make bash
WORKDIR /go/src/github.com/july2993/tk
COPY . .
RUN go build

FROM alpine:3.12
RUN apk add --no-cache tzdata bash curl socat
COPY --from=builder /go/src/github.com/july2993/tk/tk /tk
CMD [ "/tk" ]
