FROM golang:1.22.1 AS build
WORKDIR /app
COPY go.mod go.sum main.go ./
RUN go mod download
RUN GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build -tags='static' -ldflags='-s -w -linkmode external -extldflags "-static"' -o rinha ./main.go

FROM alpine:latest as release
RUN apk update && apk add --no-cache libc6-compat

EXPOSE 8080

WORKDIR /
COPY --from=build /app/rinha /rinha

CMD ["./rinha"]