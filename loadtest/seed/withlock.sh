#!/bin/zsh
# Roda o comando segurando a trava do tools/gtest.sh: seed e carga não disputam IO com testes de container.
# Uso: loadtest/seed/withlock.sh <comando...>
L=/tmp/pokelocation-gtest.lock
until mkdir $L 2>/dev/null; do
  [[ -f $L/pid ]] && ! kill -0 $(cat $L/pid) 2>/dev/null && rm -rf $L && continue
  sleep 3
done
echo $$ > $L/pid
trap 'rm -rf $L' EXIT INT TERM
"$@"
