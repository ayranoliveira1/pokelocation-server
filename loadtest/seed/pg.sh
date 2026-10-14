#!/bin/zsh
# Postgres só da auditoria (fora do compose e do testcontainers): volume nomeado, sobrevive entre rodadas.
# auto_explain e pg_stat_statements carregados; o auto_explain fica no padrão (-1, desligado) até loadtest/plans on.
# log_min_duration não pode ir na linha de comando: ela vence o ALTER SYSTEM que a coleta usa para ligar.
# Uso: pg.sh up | down | url | logs
set -e
NAME=pokeloc-s12-pg PORT=${S12_PG_PORT:-55499}
case $1 in
up)
  docker start $NAME 2>/dev/null || docker run -d --name $NAME -p 127.0.0.1:$PORT:5432 \
    -e POSTGRES_DB=s12 -e POSTGRES_USER=s12 -e POSTGRES_PASSWORD=s12 -v pokeloc-s12-pgdata:/var/lib/postgresql/data \
    postgres:17-alpine \
    -c shared_preload_libraries=pg_stat_statements,auto_explain -c pg_stat_statements.track=all \
    -c auto_explain.log_analyze=on -c auto_explain.log_buffers=on \
    -c auto_explain.log_timing=on -c auto_explain.log_format=json -c auto_explain.log_nested_statements=on \
    -c max_wal_size=4GB >/dev/null
  until docker exec $NAME pg_isready -U s12 -d s12 >/dev/null 2>&1; do sleep 1; done
  echo "postgres://s12:s12@127.0.0.1:$PORT/s12?sslmode=disable" ;;
down) docker rm -f $NAME >/dev/null && docker volume rm pokeloc-s12-pgdata >/dev/null ;;
url) echo "postgres://s12:s12@127.0.0.1:$PORT/s12?sslmode=disable" ;;
logs) docker logs $NAME ;;
*) echo "uso: $0 up|down|url|logs" >&2; exit 2 ;;
esac
