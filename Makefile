DEPS = files.go scanner.go parser.go evaluator.go main.go server.go traversers.go


all: cql.lnx rice-box.go

cross: cql.lnx cql.exe cql.mac rice-box.go

rice-box.go:
	rice embed-go

cql.exe: $(DEPS) rice-box.go
	CGO_ENABLED=0 GOOS=windows go build -o cql.exe

cql.mac: $(DEPS) rice-box.go
	CGO_ENABLED=0 GOOS=darwin go build -o cql.mac

cql.lnx: $(DEPS) rice-box.go
	CGO_ENABLED=0 GOOS=linux go build -o cql.lnx

clean:
	rm cql.mac cql.exe cql.lnx rice-box.go
