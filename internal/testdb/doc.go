// Package testdb dá Postgres e Redis reais (testcontainers) aos testes de integração; sem Docker, o teste é pulado.
//
// Um container de cada por binário de teste. Pool clona um template já migrado (dezenas de ms), então t.Parallel é
// barato. Redis entrega um dos 64 DBs lógicos, mas pub/sub não é por DB: canais precisam de nome único por teste.
//
// StoppableRedis é um container só do teste. Depois do Start a porta do host muda (o cliente devolvido segue sozinho,
// um processo de fora não) e o Redis volta vazio, como em produção sem persistência.
//
// Quem remove os containers é o Ryuk; com TESTCONTAINERS_RYUK_DISABLED=true, use
//
//	func TestMain(m *testing.M) { testdb.Main(m) }
//
// internal/db não pode usar este pacote (import circular) e sobe o próprio container.
package testdb
