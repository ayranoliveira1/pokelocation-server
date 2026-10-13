// Package testdb dá Postgres e Redis reais (testcontainers) aos testes de integração. Só para testes.
//
// Uso:
//
//	func TestAlgo(t *testing.T) {
//		pool := testdb.Pool(t)   // banco novo, já migrado (db.Migrate), apagado no fim do teste
//		rdb := testdb.Redis(t)   // DB lógico só deste teste, vazio; Close no fim do teste
//		...
//	}
//
// Sem Docker, os testes que chamam Pool/Redis são pulados (SkipIfProviderIsNotHealthy).
//
// Custo: um container postgres:17-alpine e um redis:7-alpine por binário de teste (sobem no primeiro uso). Cada Pool
// clona um template já migrado (CREATE DATABASE … TEMPLATE, ~dezenas de ms): pode usar t.Parallel à vontade.
// URL do banco do teste, para subir o binário: pool.Config().ConnString().
//
// Isolamento do Redis: cada Redis(t) pega um dos 64 DBs lógicos (FLUSHDB ao pegar). Pub/sub NÃO é por DB: use nomes
// de canal com algo único do teste. Mais de 64 testes paralelos com Redis esperam um DB liberar.
//
// "Redis cai no meio" (S7): StoppableRedis(t) dá um container só do teste, com Stop/Start. O cliente devolvido segue o
// container depois do Start (a porta do host muda; o Dialer busca a atual). Sem persistência, como em produção:
// depois do Start o Redis volta vazio.
//
// Os containers são removidos pelo Ryuk do testcontainers quando o binário termina. Com o Ryuk desligado
// (TESTCONTAINERS_RYUK_DISABLED=true), use TestMain:
//
//	func TestMain(m *testing.M) { testdb.Main(m) }
//
// O pacote db não usa testdb (seria import circular); internal/db/db_test.go sobe o próprio container.
package testdb
