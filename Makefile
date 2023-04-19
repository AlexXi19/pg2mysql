CGO_ENABLED=0

all: build
build: 
	go build -o pg2mysql cmd/pg2mysql/main.go
linux:
	GOOS=linux GOARCH=amd64 go build -o pg2mysql_linux cmd/pg2mysql/main.go

clean:
	rm pg2mysql_linux
