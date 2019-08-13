DEPS = *.go


all: rice-box.go
SRC = evaluator.go files.go main.go parser.go scanner.go selector.go server.go treeProcess.go utils.go value.go

#cross: cql.lnx cql.exe cql.mac rice-box.go

rice-box.go:
	rice embed-go
	zip -r ./csv.zip ./*.go ./demo.png
	scp csv.zip dave@davosaur.com:/home/dave/h/csvci/

#cql.exe: $(DEPS) rice-box.go
#	CGO_ENABLED=0 GOOS=windows go build -o cql.exe

#cql.mac: $(DEPS) rice-box.go
#	CGO_ENABLED=0 GOOS=darwin go build -o cql.mac

#cql.lnx: $(DEPS) rice-box.go
#	CGO_ENABLED=0 GOOS=linux go build -o cql.lnx

run: $(SRC)
	go run $(SRC)

#clean:
#	rm cql.mac cql.exe cql.lnx rice-box.go
