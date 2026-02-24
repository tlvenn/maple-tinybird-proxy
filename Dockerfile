FROM golang:1.22-alpine AS builder
WORKDIR /build
COPY go.mod .
COPY *.go .
RUN go build -ldflags="-s -w" -o proxy .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /build/proxy /proxy
EXPOSE 7181
ENTRYPOINT ["/proxy"]
