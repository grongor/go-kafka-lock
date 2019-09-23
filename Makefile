BIN = ${PWD}/bin

.PHONY: build
build:
	for CMD in `ls cmd`; do \
		go build ./cmd/$$CMD; \
	done

.PHONY: check
check: mocks lint cs vet staticcheck test

.PHONY: lint
lint: tools
	$(BIN)/golint --set_exit_status ./...

.PHONY: cs
cs: tools
	diff=$$($(BIN)/goimports -d . ); test -z "$$diff" || (echo "$$diff" && exit 1)

.PHONY: cs-fix
cs-fix: format

.PHONY: format
format: tools
	$(BIN)/goimports -w .

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test: mocks
	timeout 10 go test
	timeout 60 go test --race
	timeout 120 go test --count 1000

.PHONY: mocks
mocks: tools
	$(BIN)/mockgen --destination mocks/sarama.go --package mocks github.com/Shopify/sarama ClusterAdmin,Client,ConsumerGroup,ConsumerGroupSession,ConsumerGroupClaim
	$(BIN)/mockgen --destination mocks/flap_guard.go --package mocks --source flap_guard.go
	$(BIN)/goimports -w mocks

.PHONY: staticcheck
staticcheck: tools
	$(BIN)/staticcheck ./...

.PHONY: tools
tools: bin

bin: export GOBIN = $(BIN)
bin:
	go install github.com/golang/mock/mockgen
	go install golang.org/x/lint/golint
	go install golang.org/x/tools/cmd/goimports
	go install honnef.co/go/tools/cmd/staticcheck
