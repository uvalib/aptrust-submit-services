GOCMD = go
GOBUILD = $(GOCMD) build
GOCLEAN = $(GOCMD) clean
GOTEST = $(GOCMD) test
GOGET = $(GOCMD) get
GOMOD = $(GOCMD) mod
GOFMT = $(GOCMD) fmt
GOVET = $(GOCMD) vet
PACKAGENAME = aptrust-submit-validator
BINNAME = $(PACKAGENAME)
COMMON = ../service-common

build: common darwin-bin

linux: common linux-bin

all: common darwin-bin linux-bin

darwin-bin:
	GOOS=darwin GOARCH=amd64 $(GOBUILD) -a -race -o bin/$(BINNAME).darwin *.go

linux-bin:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) -a -installsuffix cgo -o bin/$(BINNAME).linux *.go

common:
	-ln -s $(COMMON)/helpers.go . 2> /dev/null || true
	-ln -s $(COMMON)/main-cmdline.go . 2> /dev/null || true
	-ln -s $(COMMON)/sqs.go . 2> /dev/null || true
	-ln -s $(COMMON)/version.go . 2> /dev/null || true

clean:
	$(GOCLEAN) .
	rm -rf bin

dep:
	$(GOGET) -u
	$(GOMOD) tidy
	$(GOMOD) verify

fmt:
	$(GOFMT)

vet:
	$(GOVET)

check:
	go install honnef.co/go/tools/cmd/staticcheck
	$(HOME)/go/bin/staticcheck -checks all,-S1002,-ST1003 *.go
	go install golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow
	$(GOVET) -vettool=$(HOME)/go/bin/shadow ...
