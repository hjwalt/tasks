MODULE=github.com/hjwalt/tasks

test:
	go test ./... -cover -coverprofile cover.out
	
testv:
	go test ./... -cover -coverprofile cover.out -v

cov: test
	go tool cover -func cover.out

htmlcov: test
	go tool cover -html cover.out -o cover.html

# --------------------

tidy:
	go mod tidy
	go fmt ./...

update:
	go get -u ./...
	go mod tidy
	go fmt ./...

# --------------------

run:
	./script/run.sh

# --------------------

proto: RUN
	rm -rf $$GOPATH/$(MODULE)/ ;\
	protoc -I=. --go_out=$$GOPATH **/*.proto ;\
	cp -r $$GOPATH/$(MODULE)/* .

# --------------------

up: RUN
	podman-compose up -d

down: RUN
	podman-compose down

# --------------------

RUN:
