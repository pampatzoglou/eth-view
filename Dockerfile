# Development stage
ARG GO_VERSION=1.21
FROM golang:${GO_VERSION}-alpine AS development

WORKDIR /app
COPY . .

RUN go mod download

# Command to run the application in development mode
CMD ["go", "run", "main.go"]

