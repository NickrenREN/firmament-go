all-build:
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o firmament ./cmd/firmament
.PHONY: all-build