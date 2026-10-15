#!/usr/bin/env bash
# Dump do Postgres para o Object Storage da Oracle por uma PAR de escrita no bucket (BACKUP_PAR_URL no .env, termina
# em /o/; vazio = desligado). A retenção é a regra de lifecycle do bucket. Restore: deploy/README.md.
set -euo pipefail
cd "$(dirname "$0")/.."
url=$(sed -n 's/^BACKUP_PAR_URL=//p' .env)
[ -n "$url" ] || exit 0
f=$(mktemp)
trap 'rm -f "$f"' EXIT
docker compose exec -T postgres pg_dump -U pokelocation --clean --if-exists --no-owner pokelocation | gzip >"$f"
# arquivo, não stream: o PutObject da Oracle exige Content-Length
# URL pela entrada padrão, não no argv: a PAR é segredo e o argv aparece no ps
printf 'url = "%s"\n' "${url}pokelocation-$(date -u +%Y%m%dT%H%MZ).sql.gz" | curl -fsS --retry 3 -T "$f" -K -
