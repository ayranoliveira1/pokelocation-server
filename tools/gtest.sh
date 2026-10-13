#!/bin/zsh
# Uso: tools/gtest.sh <args do go test...>   ex.: tools/gtest.sh -race ./internal/relay/
# Testes que sobem container (testcontainers) rodam UM POR VEZ na máquina inteira: os outros esperam a vez.
# Testes puros (sem container) podem usar go test direto.
P=${0:a:h:h}
L=/tmp/pokelocation-gtest.lock
until mkdir $L 2>/dev/null; do
  # lock órfão: dono morreu sem limpar
  [[ -f $L/pid ]] && ! kill -0 $(cat $L/pid) 2>/dev/null && rm -rf $L && continue
  sleep 3
done
echo $$ > $L/pid
# remove só containers de teste PARADOS (órfãos); os que rodam são de outro processo ou o ryuk deles limpa
trap 'docker ps -aq --filter label=org.testcontainers=true --filter status=exited --filter status=created | xargs -r docker rm -f >/dev/null 2>&1; rm -rf $L' EXIT INT TERM
cd $P && go test -p 2 "$@"
