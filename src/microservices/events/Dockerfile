FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY . .

RUN go mod tidy && go build -o events-service main.go

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/events-service .

EXPOSE 8082

ENV PORT=8082

CMD ["./events-service"] 