FUZZTIME ?= 60s
-include .env
export

.PHONY: test fuzz lint run ca

test:
	go test -race ./...

# Cada FuzzXxx de cada pacote por $(FUZZTIME).
fuzz:
	@for pkg in $$(go list ./...); do \
		for f in $$(go test -list '^Fuzz' $$pkg | grep '^Fuzz'); do \
			go test -run '^$$' -fuzz "^$$f$$" -fuzztime $(FUZZTIME) $$pkg || exit 1; \
		done; \
	done

# gosec fixado num commit de master: a v2.29.0 não lê o export data do Go 1.27. Troque pela próxima tag.
lint:
	go vet ./...
	go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
	go run github.com/securego/gosec/v2/cmd/gosec@v2.29.1-0.20261012130337-71577268350e ./...

# Postgres e Redis do compose; o servidor roda local.
run:
	docker compose up -d postgres redis
	DATABASE_URL=postgres://pokelocation:$(POSTGRES_PASSWORD)@127.0.0.1:$${PG_PORT:-5432}/pokelocation?sslmode=disable \
	REDIS_URL=redis://127.0.0.1:$${REDIS_PORT:-6379}/0 \
	go run ./cmd/server

# CA local do Caddy (tls internal, SITE_ADDRESS=localhost): instale SÓ no emulador. Não vai para o git.
ca:
	docker compose cp caddy:/data/caddy/pki/authorities/local/root.crt caddy-local-root.crt
	@openssl x509 -in caddy-local-root.crt -noout -subject -enddate
