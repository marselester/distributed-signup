build:
	go build ./cmd/pg-ctl
	go build ./cmd/signup-server
	go build ./cmd/signup-ctl

test:
	go test ./...
