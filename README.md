# pokelocation-server

Servidor da camada social do [PokeLocation](https://github.com/ayranoliveira1/pokelocation): contas, amigos, chaves
públicas do Signal, repasse de mensagens cifradas, posições dos amigos e o proxy do PoGo Coordinates. O servidor nunca
vê conteúdo nem posição: só repassa pacotes cifrados ponta a ponta.

Go 1.27, um binário só (REST + WebSocket), PostgreSQL e Redis. TLS pelo Caddy no local e pelo nginx em produção.

## Versões

### v1.1: proxy do PoGo Coordinates (para o app v1.2)

- **Proxy do PoGo Coordinates** (`/v1/pogo/*`): login, lookup, search, reveal e treinador. O token do PoGo do usuário
  fica guardado cifrado.
- **Tokens do servidor em rodízio:** lookup, search, reveal e alertas sem shiny funcionam sem o usuário vincular
  conta. Token recusado sai do rodízio por 5 min.
- **Alertas por push** com foto, forma e costume do Pokémon; shiny só com treinador vinculado.
- **PoGo lento:** espera até 30 s em lookup e search, depois responde `504 upstream_timeout`.
- **Limites** por IP no nginx (login mais apertado) e por usuário nas rotas autenticadas.

O app v1.1 continua funcionando igual com este servidor.

### v1.0: camada social

Contas por e-mail e senha ou Google (PKCE), com @nome único; amigos com pedidos, bloquear e desbloquear; chaves
públicas do Signal; repasse de mensagens 1:1 e de grupo com fila para quem está offline (até 1000 por destinatário);
grupos; posição dos amigos guardada cifrada por 60 s; push pelo FCM; excluir a conta; página `/privacy`; deploy
automático na `main` que volta ao commit anterior se a subida falhar.

Cada versão tem a sua branch `release/vX.Y` e o seu [release](https://github.com/ayranoliveira1/pokelocation-server/releases).

## Rodar

```sh
cp .env.example .env          # defina POSTGRES_PASSWORD
make run                      # Postgres e Redis no Docker, servidor local em :8080
docker compose up --build     # tudo no Docker, Caddy em :443
```

Produção (nginx, deploy automático, /privacy): [`deploy/README.md`](deploy/README.md).

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
| `FCM_CREDENTIALS` | vazio = push falso (só loga `fcm ping`) | caminho do JSON da conta de serviço do Firebase (deploy) |
| `POGO_TOKEN_KEY` | vazio = PoGo desligado (`/v1/pogo/*` → 503) | chave AES-256 dos tokens do PoGo Coordinates, 32 bytes em base64 (`openssl rand -base64 32`). Trocar invalida os vínculos (o app pede login de novo) |
| `POGO_WORKERS` | `32` | alertas do PoGo em paralelo por instância (1–64). Capacidade ≈ workers × intervalo / latência do PoGo, por regra: o PoGo leva 5–15 s por chamada (~7 s típico), então 32 com 3 min ≈ 850 usuários com 1 alerta (≈85 com 10 regras; ≈380 com o PoGo em 15 s), 64 ≈ 1.650. O log `pogo: fila atrasada` avisa quando falta. Mais workers = mais pressão no PoGo |
| `POGO_ALERT_INTERVAL` | `3m` | de quanto em quanto tempo cada usuário com alerta roda (2m–10m; fora disso a subida falha). Maior = mais usuários por instância, alerta mais tardio |
| `POGO_SHARED_TOKENS` | vazio = tudo com o token do usuário | tokens do PoGo do servidor, separados por vírgula (exige `POGO_TOKEN_KEY`). Lookup, search, reveal e alertas sem shiny vão por eles em rodízio, sem exigir vínculo; shiny, `/me` e treinador seguem com o token do usuário. Token recusado (401, 402, 403 da conta) fica 5 min fora do rodízio e no 429 até o `retry_after`; 403 de filtro (`vip_required`, `trainer_not_linked`, `shiny_account_unavailable`) volta ao app; todos recusados: com vínculo tenta o token do usuário, sem vínculo 502. Log `pogo: token do servidor recusado` com `pos` (posição na lista, a partir de 0) = token a trocar |
| `POGO_UPSTREAM_URL` | `https://coordinates-api.pokemongopro.com` | só para teste local com um PoGo falso |
| `LOG_REQUESTS` | vazio = desligado | `1` = log de cada requisição (método, rota sem query, status, ms), só para depurar no local |
| `PPROF_ADDR` | vazio = desligado | endereço **interno** do pprof (ex.: `127.0.0.1:6060`) |
| `CLUSTER` | vazio = uma instância | `1` = várias instâncias atrás do mesmo Redis (repasse do WS por pub/sub). Cada instância abre até 32 conexões no Postgres (`pool_max_conns` no `DATABASE_URL` muda): instâncias × pool < `max_connections` (100 por padrão). Os limites por IP e por conta (login, refresh, rotas autenticadas) são por instância: com `CLUSTER=1`, levar o do auth para o Redis antes |
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

MIT.
