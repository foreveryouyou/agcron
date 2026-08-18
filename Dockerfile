FROM golang:1.24 AS build
WORKDIR /src
ENV GOPROXY="https://goproxy.cn|https://proxy.golang.com.cn"
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/server ./examples/server

FROM debian:stable-slim
COPY --from=build /out/server /server
EXPOSE 8080
ENTRYPOINT ["/server"]
