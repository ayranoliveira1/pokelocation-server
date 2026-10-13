# pokelocation-server

Servidor da camada social do [PokeLocation](https://github.com/ayranoliveira1/pokelocation): contas, amigos, chaves
públicas do Signal, repasse de mensagens cifradas e posições dos amigos. O servidor nunca vê conteúdo nem posição:
só repassa pacotes cifrados ponta a ponta.

Go 1.27, um binário só (REST + WebSocket), PostgreSQL, Redis e Caddy (TLS). Contrato com o app: [`docs/api.md`](docs/api.md).

## Rodar

```sh
cp .env.example .env          # defina POSTGRES_PASSWORD
make run                      # Postgres e Redis no Docker, servidor local em :8080
docker compose up --build     # tudo no Docker, Caddy em :443
```

## Configuração (variáveis de ambiente)

| Variável | Padrão | |
|---|---|---|
| `DATABASE_URL` | — (obrigatória) | `postgres://…` |
| `ADDR` | `:8080` | endereço HTTP interno |
| `GOMEMLIMIT` | `512MiB` no compose | teto de memória do runtime |

As migrações (`internal/db/migrations`) rodam sozinhas na subida.

## Testes

```sh
make test   # go test -race ./... (integração com Postgres/Redis reais via testcontainers: precisa de Docker)
make fuzz   # cada FuzzXxx por 60 s (FUZZTIME=…)
make lint   # go vet, govulncheck, gosec
```

## Licença

MIT. O app é AGPL-3.0 por causa da libsignal; o servidor não usa a libsignal.
