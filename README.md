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

### App no emulador (desenvolvimento local)

HTTPS de verdade também no local: o Caddy usa `tls internal` com `SITE_ADDRESS=localhost` (padrão).

```sh
docker compose up -d --build
make ca                              # copia a CA local para caddy-local-root.crt
adb reverse tcp:8443 tcp:443         # o emulador fala com https://localhost:8443
```

Instale `caddy-local-root.crt` só no emulador; só a build debug do app confia em CA de usuário. No `.env`,
`PUBLIC_URL=https://localhost:8443`. Mudou uma migração ainda não commitada? Recrie só o banco
(`docker compose down && docker volume rm pokelocation-server_pgdata`): o `down -v` apaga também a CA do Caddy, e aí
é preciso rodar `make ca` e reinstalar no emulador.

## Configuração (variáveis de ambiente)

| Variável | Padrão | |
|---|---|---|
| `DATABASE_URL` | — (obrigatória) | `postgres://…` |
| `REDIS_URL` | — (obrigatória) | `redis://…` |
| `AUTH_SIGNING_KEY` | — (obrigatória) | seed Ed25519 de 32 bytes em base64 (`openssl rand -base64 32`) |
| `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` | vazio = login Google desligado | |
| `PUBLIC_URL` | — | `https://<host>` visto pelo navegador (redirect do Google) |
| `LOG_REQUESTS` | vazio = desligado | `1` = log de cada requisição (método, rota sem query, status, ms), só para depurar no local |
| `PPROF_ADDR` | vazio = desligado | endereço **interno** do pprof (ex.: `127.0.0.1:6060`) |
| `CLUSTER` | vazio = uma instância | `1` = várias instâncias atrás do mesmo Redis (repasse do WS por pub/sub). Cada instância abre até 32 conexões no Postgres (`pool_max_conns` no `DATABASE_URL` muda): instâncias × pool < `max_connections` (100 por padrão) |
| `ADDR` | `:8080` | endereço HTTP interno |
| `GOMEMLIMIT` | `1GiB` no compose | ~70% da RAM da máquina/container; abaixo do pico vivo (~37 KB por conexão) o GC roda sem parar. `GOGC` fica no padrão |

As migrações (`internal/db/migrations`) rodam sozinhas na subida, cada uma na sua transação. Migração commitada não
se reescreve: mudança vai numa nova.

**Índice novo em tabela que já existe** vai numa migração que começa com `-- migrate:no-transaction`, com
`CREATE INDEX CONCURRENTLY IF NOT EXISTS` (não trava escrita; roda fora de transação, um comando por vez; um índice
deixado INVALID por uma falha é apagado e recriado na próxima subida). Índice criado junto da própria tabela, na mesma
migração, segue transacional. Numa migração no-transaction, a versão só é gravada no fim: **todo** comando tem de ser
idempotente (`IF NOT EXISTS`/`IF EXISTS`), e nenhum pode ter `;` no meio (string, corpo de função), porque os comandos
são separados pelo `;` do fim da linha.
ponytail: coluna nova com `CHECK` na mesma linha valida a tabela inteira sob lock exclusivo (ok com tabela pequena; a
0007 limita a espera com `lock_timeout`). Com tabela grande: `ADD CONSTRAINT … NOT VALID` e depois `VALIDATE CONSTRAINT`.

## Testes

```sh
make test   # tools/gtest.sh -race ./... (integração com Postgres/Redis reais via testcontainers: precisa de Docker)
make fuzz   # cada FuzzXxx por 60 s (FUZZTIME=…)
make lint   # go vet, govulncheck, gosec
```

Teste que sobe container roda **só** por `tools/gtest.sh <args do go test>`: trava global (um por vez na máquina,
`-p 2`) e remoção dos containers de teste parados no fim. Teste puro, sem container, pode usar `go test` direto.

## Licença

MIT. O app é AGPL-3.0 por causa da libsignal; o servidor não usa a libsignal.
