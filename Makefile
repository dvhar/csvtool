DEPS = files.go  interpreter.go  main.go  server.go


all: sql.lnx rice-box.go

cross: sql.lnx sql.exe sql.mac rice-box.go

rice-box.go:
	rice embed-go

sql.exe: $(DEPS) rice-box.go
	CGO_ENABLED=0 GOOS=windows go build -o sql.exe

sql.mac: $(DEPS) rice-box.go
	CGO_ENABLED=0 GOOS=darwin go build -o sql.mac

sql.lnx: $(DEPS) rice-box.go
	CGO_ENABLED=0 GOOS=linux go build -o sql.lnx

clean:
	rm sql.mac sql.exe sql.lnx rice-box.go

test:
	go run files.go main.go server.go parser.go preparser.go scanner.go
