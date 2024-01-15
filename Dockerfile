FROM golang:alpine AS builder
LABEL maintainer="Mike Jansen <mjans71@pm.me>"
LABEL org.opencontainers.image.source=https://github.com/mikejansen/redis-app
LABEL org.opencontainers.image.description="A simple Go web app that implements cache-aside with Redis and MySQL"
LABEL org.opencontainers.image.licenses=MIT

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o redis-app .

FROM scratch
COPY --from=builder /app/redis-app .
EXPOSE 8080
CMD ["./redis-app"]
