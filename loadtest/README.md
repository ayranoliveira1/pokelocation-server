# Teste de carga (spec §8, plano S6/S7)

Meta: **15 mil conexões, ≤ 25 KB/conexão, p99 < 50 ms, 0 perdas** (todo envelope chega e leva `Ack`).

O gerador (`loadtest/main.go`) semeia usuários `lt-*@load.test` e amizades em anel direto no Postgres e assina os
tokens com a `AUTH_SIGNING_KEY` (`tv` = 0). Depois abre N conexões WS autenticadas, manda `Location` a cada 5 s e um
`Envelope` por usuário por minuto a um amigo. Mede latência ponta a ponta (timestamp no payload), perdas e Acks, RSS e
CPU do servidor (pelo `ps` do pid), e no fim imprime a tabela em markdown, no formato do benchmark do spike. Sai com
código 1 se alguma meta falhar e apaga os usuários semeados (`-keep` mantém).

Só com a máquina ociosa e combinado com o Ferreiro. Sem containers de teste: usa o Postgres e o Redis do compose.

## Roteiro

Tudo na raiz do repositório, num terminal só (as variáveis valem para os passos seguintes).

```sh
cd /Users/ayranoliveira/Documents/Projetos/pokelocation-server
set -a; . ./.env; set +a     # POSTGRES_PASSWORD, PG_PORT, REDIS_PORT, AUTH_SIGNING_KEY

# 1. Postgres e Redis do compose (o servidor roda nativo, fora do Docker, para o -pid medir o RSS certo)
docker compose up -d postgres redis
export DATABASE_URL="postgres://pokelocation:${POSTGRES_PASSWORD}@127.0.0.1:${PG_PORT:-5432}/pokelocation?sslmode=disable"
export REDIS_URL="redis://127.0.0.1:${REDIS_PORT:-6379}/0"

# 2. Binários do HEAD, sem -race
go build -trimpath -o /tmp/pokeloc-server ./cmd/server
go build -trimpath -o /tmp/pokeloc-loadtest ./loadtest

# 3. Servidor nativo: GOMEMLIMIT igual ao do compose, GOMAXPROCS padrão. ADDR ":8080" ouve em 127.0.0.1 e ::1
#    (o gerador divide as conexões entre os dois: são ~16 mil portas efêmeras por destino no macOS).
ADDR=:8080 GOMEMLIMIT=512MiB /tmp/pokeloc-server > /tmp/pokeloc-server.log 2>&1 &
SP=$!
sleep 2; curl -fsS http://127.0.0.1:8080/healthz && echo " servidor $SP no ar"

# 4a. Ensaio curto (confere token, WS e contas, ~40 s)
/tmp/pokeloc-loadtest -n 500 -warm 5s -measure 15s -pid $SP

# 4b. Carga de verdade (~2–3 min: conectar, 20 s de aquecimento, 60 s de medição, 10 s de dreno)
/tmp/pokeloc-loadtest -n 15000 -pid $SP | tee /tmp/carga-15k.log

# 5. Limpar
kill -TERM $SP; wait $SP
docker compose exec -T postgres psql -U pokelocation -tAc \
  "SELECT count(*) FROM users WHERE email LIKE 'lt-%@load.test'"   # tem de dar 0 (o gerador apaga no fim)
rm -f /tmp/pokeloc-server /tmp/pokeloc-loadtest
```

Entre duas rodadas, espere ~1 min: as 15 mil portas do cliente ficam em `TIME_WAIT`. Se uma rodada for interrompida,
a próxima apaga os `lt-*` que sobraram antes de semear.

## Flags úteis

| Flag | Padrão | |
|---|---|---|
| `-n` | 15000 | conexões (1 usuário cada) |
| `-friends` | 10 | amigos por usuário (cada posição vai a 10 conexões: ~30 mil `LocationUpdate`/s com 15 mil) |
| `-msg` | 60s | 1 envelope por usuário a cada intervalo (0 = sem chat) |
| `-warm` / `-measure` / `-drain` | 20s / 60s / 10s | aquecimento, janela medida, espera pelos envelopes em trânsito |
| `-pid` | 0 | pid do servidor: RSS (ocioso, conectado, pico) e CPU média na janela |
| `-url` | `ws://127.0.0.1:8080/v1/ws,ws://[::1]:8080/v1/ws` | destinos em rodízio; acrescente mais para passar de ~32 mil |
| `-max-kb` / `-p99` | 25 / 50ms | metas |

Como medir: **RAM por conexão** = (pico − ocioso) / conexões, como no spike. **CPU** = tempo de CPU do pid
consumido na janela ÷ duração da janela (em núcleos). Latência de posição é amostrada em 10% (com 15 mil são ~30 mil
`LocationUpdate`/s); a de envelope cobre todos. O gerador roda na mesma máquina e disputa CPU com o servidor, como
no spike: a latência medida é um teto.
