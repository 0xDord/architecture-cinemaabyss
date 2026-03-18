FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY . .

RUN go mod tidy && go build -o proxy-service main.go

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/proxy-service .

EXPOSE 8000

ENV PORT=8000

CMD ["./proxy-service"]