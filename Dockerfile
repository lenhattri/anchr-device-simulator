FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod .
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/anchr-simulator ./

FROM alpine:3.20
RUN addgroup -S simulator && adduser -S simulator -G simulator
WORKDIR /app
COPY --from=builder /out/anchr-simulator /usr/local/bin/anchr-simulator
COPY config.yaml /app/config.yaml
COPY config.json /app/config.json
USER simulator
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/anchr-simulator"]
CMD ["-config", "/app/config.json"]
