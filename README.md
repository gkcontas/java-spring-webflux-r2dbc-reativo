# Stack reativa contra stack bloqueante, na mesma aplicação

Este projeto é uma aplicação só, com as duas stacks dentro: WebFlux + R2DBC
sobre Netty, e Spring MVC + JDBC sobre Tomcat. A troca é uma propriedade —
`spring.main.web-application-type` — e abaixo do controller tudo é
compartilhado: a mesma SQL, as mesmas validações, o mesmo contrato de erro.
Assim a diferença medida é da stack, e não de duas aplicações diferentes
escritas por pessoas diferentes em semanas diferentes.

O resultado curto: **reativo não deixa nada mais rápido.** Remove um limite.
Abaixo dele as duas stacks são indistinguíveis — 501 ms contra 501 ms. Acima,
a bloqueante enfileira e vai a 4180 ms de mediana contra 500 ms. E existe o
caso em que a reativa fica **muito pior** que o pool que ela substituiu: uma
chamada bloqueante dentro do event loop derruba o teto para 24 req/s. Esse
caso também está medido aqui.

Há um resultado que contraria o que se costuma dizer, e ele ficou no README
porque foi medido: o `fetchSize` do R2DBC **não** é a diferença entre memória
constante e carregar o resultado inteiro. O driver já aplica backpressure no
socket. O que de fato estoura o heap é `collectList`.

## Tecnologias e bibliotecas

| Tecnologia | Para que serve aqui |
| --- | --- |
| Java 21 | virtual threads no gerador de carga; a aplicação não depende delas |
| Spring Boot 3.5 | as duas stacks; a escolha entre elas é configuração dele |
| Spring WebFlux + Reactor | lado reativo, sobre Netty |
| Spring MVC | lado bloqueante, sobre Tomcat |
| Spring Data R2DBC (`DatabaseClient`) | acesso reativo ao PostgreSQL |
| r2dbc-postgresql | driver reativo; o `fetchSize` vem do SPI dele |
| Spring JDBC (`JdbcTemplate`) | acesso bloqueante, com a mesma SQL |
| Flyway | migrations — que rodam por JDBC, porque não existe Flyway R2DBC |
| Micrometer + Actuator | métricas e a leitura de heap usada nas medições |
| BlockHound | detecção de chamada bloqueante em thread não-bloqueante |
| Testcontainers | PostgreSQL real nos testes de integração |
| JUnit 5 + AssertJ + `StepVerifier` | testes, inclusive os de backpressure |

## Pré-requisitos

- Java 21
- Docker e Docker Compose

## Como rodar

Suba o PostgreSQL:

```bash
docker compose up -d
```

Na configuração reativa, que é o padrão:

```bash
./gradlew bootRun
```

Na bloqueante:

```bash
./gradlew bootRun --args='--spring.profiles.active=blocking'
```

Para derrubar tudo:

```bash
docker compose down -v
```

## Endpoints

As rotas, os payloads e os códigos de status são os mesmos nas duas stacks. Só
`/stations/summary` responde diferente, e é justamente o ponto: a versão
bloqueante não tem operador para escolher.

| Rota | O que faz |
| --- | --- |
| `GET /runtime` | diz qual stack está no ar, com servidor, transaction manager e threads |
| `GET /stations` | lista as estações |
| `GET /stations/{code}` | uma estação, ou 404 no contrato de erro do projeto |
| `GET /stations/summary?codes=a,b&mode=flatMap` | fan-out por estação, composto pelo operador escolhido |
| `GET /readings/stream?stationCode=&limit=&fetchSize=` | export em NDJSON, uma linha por leitura |
| `GET /readings/all?stationCode=&limit=` | as mesmas linhas materializadas num array |
| `POST /readings` | grava uma leitura, validada e transacional |
| `POST /readings/failing` | grava e falha, para o rollback ser observável |
| `POST /admin/readings?rowsPerStation=` | gera volume para as medições |
| `GET /admin/readings/count` | total de leituras |
| `GET /work/downstream?delayMs=500` | espera numa dependência lenta |
| `GET /work/db?delayMs=100` | espera dentro do pool de conexões |
| `GET /context/request-id` | onde o id da requisição sobreviveu |

Só na stack reativa:

| Rota | O que faz |
| --- | --- |
| `GET /trap/blocking?delayMs=500` | a espera bloqueante dentro do event loop |
| `GET /trap/blocking-elastic?delayMs=500` | a mesma espera em `boundedElastic` |
| `GET /trap/block-call` | `block()` numa thread não-bloqueante |

```bash
curl localhost:8080/runtime
curl localhost:8080/stations/BSB-01
curl "localhost:8080/stations/summary?codes=BSB-01,RIO-01,SPO-01&mode=concatMap"
curl "localhost:8080/readings/stream?stationCode=BSB-01&limit=5"
curl -X POST localhost:8080/readings -H 'Content-Type: application/json' \
  -d '{"stationCode":"BSB-01","recordedAt":"2026-08-01T10:00:00Z",
       "temperatureC":25.5,"humidityPct":60}'
curl -H 'X-Request-Id: abc-123' localhost:8080/context/request-id
```

## Setup das medições

Tudo numa sessão, com o jar empacotado (`./gradlew bootJar`), daemon do Gradle
parado e chamadas de aquecimento descartadas. Máquina de 12 núcleos, PostgreSQL
16 em container, aplicação e banco na mesma máquina.

A carga vem de `tools/LoadGenerator.java`, um arquivo único rodado com
`java LoadGenerator.java`, em **modelo aberto**: uma requisição parte a cada
`1/taxa` segundo, independentemente de as anteriores terem terminado. É o único
modelo que mostra uma stack enfileirando — num modelo fechado, quando o servidor
fica mais lento o cliente envia menos, e a latência de um servidor saturado
parece a de um servidor ocioso.

A dependência lenta é simulada em processo (`Mono.delay` de um lado,
`Thread.sleep` do outro) em vez de ser uma chamada HTTP real. Uma chamada real
traria o modelo de thread de um segundo serviço para dentro de cada número, e a
pergunta aqui é o que a stack de quem chama faz enquanto espera.

As medições de heap vêm de `jvm.memory.used{area=heap}` do Actuator, amostrado a
cada 200 ms durante a requisição, com o heap limitado a 256 MB.

## 1. Espera numa dependência lenta — onde reativo ganha

`GET /work/downstream?delayMs=500`. Tomcat com 200 threads; Netty com 12 loops.

| Taxa | Concorrência | Reativa (Netty) | Bloqueante (Tomcat) |
| --- | --- | --- | --- |
| 200/s | 100 | mediana 501 ms · p99 510 ms | mediana 501 ms · p99 506 ms |
| 600/s | 300 | mediana 500 ms · p99 518 ms | **mediana 4180 ms · p99 7872 ms** |

Threads de SO depois da carga: **48 na reativa, 216 na bloqueante.**

Abaixo do teto do pool as duas são a mesma coisa, e qualquer benchmark que pare
aqui conclui que reativo não serve para nada. Acima do teto a bloqueante não
fica mais lenta por estar trabalhando mais — ela enfileira, porque não há thread
para atender. A reativa não tem o que enfileirar: ninguém está sentado na espera.

## 2. A inversão — bloquear o event loop

`GET /trap/blocking?delayMs=500`. Uma chamada bloqueante dentro do loop. É o que
um driver sem implementação reativa, um `RestTemplate` esquecido numa migração ou
um appender de log escrevendo em disco lento fazem com a aplicação.

São 12 loops nesta máquina, e cada requisição prende um por 0,5 s: o teto é
**24 req/s**.

| Taxa | Mediana | p99 |
| --- | --- | --- |
| 12/s (metade do teto) | 503 ms | 921 ms |
| 24/s (no teto) | 773 ms | 1406 ms |
| 48/s (duas vezes o teto) | **5489 ms** | **15674 ms** |

O mesmo servidor atende `/work/downstream` a 600/s com 500 ms de mediana — 25
vezes a taxa, dez vezes mais rápido. E o pool de 200 threads que foi substituído
atende 200/s com 501 ms. Uma linha bloqueante no lugar errado não degrada a
aplicação: ela inverte o resultado da migração.

O teto é mesmo a contagem de loops, e não outra coisa. Com `/trap/blocking` a
48/s, variando `reactor.netty.ioWorkerCount`:

| Loops | Teto teórico | Mediana a 48/s |
| --- | --- | --- |
| 4 | 8/s | 31458 ms |
| 12 (padrão) | 24/s | 5489 ms |
| 24 | 48/s | 889 ms |

### O escape hatch, e o que ele é

`subscribeOn(Schedulers.boundedElastic())` tira a chamada do loop e devolve o
comportamento. Mas vale olhar para o que foi construído: `boundedElastic` é um
pool limitado a 10 threads por núcleo — 120 aqui — na frente de uma chamada
bloqueante. Ou seja, a arquitetura que havia antes, com uma API reativa em cima.
E ele tem o seu próprio teto, 120 / 0,5 s = 240/s:

| Taxa | Mediana | p99 |
| --- | --- | --- |
| 48/s | 503 ms | 507 ms |
| 400/s | **5484 ms** | 10385 ms |

As threads vivas do processo saem de 48 para **168** quando o pool enche.

## 3. Espera dentro do banco — onde reativo não muda nada

`GET /work/db?delayMs=100`, com pool de 10 conexões nas duas stacks. O teto é
10 / 0,1 s = 100/s, e é o mesmo teto para as duas.

| Taxa | Reativa | Bloqueante |
| --- | --- | --- |
| 50/s | mediana 106 ms | mediana 104 ms |
| 200/s | mediana 8037 ms | mediana 7857 ms |

Reativo remove a thread como recurso escasso, não a conexão. Um serviço cujo
gargalo é o banco não melhora com a troca de stack — melhora com mais conexões,
consultas mais baratas ou menos consultas.

## 4. Export de 500.000 linhas, heap de 256 MB

| Forma | Stack | Pico de heap | Resultado |
| --- | --- | --- | --- |
| stream, `fetchSize=500` | reativa | 54 MB | 200 em 8,7 s |
| stream, `fetchSize=0` | reativa | 82 MB | 200 em 8,1 s |
| `collectList` | reativa | 245 MB | **OutOfMemoryError, sem resposta** |
| cursor + `StreamingResponseBody` | bloqueante | 59 MB | 200 em 1,5 s |
| `List` materializada | bloqueante | 223 MB | 200 em 1,6 s |

Três coisas saem daqui, e duas contrariam o que se espera.

**O `fetchSize` não é o que separa memória constante de resultado inteiro.** Sem
cursor o pico foi 82 MB, não 250: o r2dbc-postgresql aplica backpressure no
próprio socket, então quem acumula é o servidor, não o cliente. O cursor ainda
vale — 54 MB contra 82 MB, e o backend do Postgres não fica preso num envio —
mas o ganho é de margem, não de ordem de grandeza. Com cliente lento
(limitado a 2 MB/s) os dois ficaram na mesma faixa.

**Quem estoura o heap é `collectList`.** Um `Flux` na assinatura não protege de
nada: `collectList` materializa, e 500 mil linhas passam dos 256 MB e matam a
requisição. A forma materializada da stack bloqueante chega perto (223 MB) e
sobrevive, porque o caminho reativo mapeia a linha duas vezes — entidade e DTO —
e carrega a máquina de operadores em cima.

**Streaming não é o que a stack reativa compra.** JDBC faz streaming: fetch size,
callback por linha e `StreamingResponseBody`, e o heap fica em 59 MB. A
diferença é qual forma a API torna fácil — e, nesse export, o caminho
bloqueante foi **5,8 vezes mais rápido** (1,5 s contra 8,7 s), porque cada
elemento do `Flux` paga codificação e alocação de buffer que a escrita direta
no `OutputStream` não paga. Reativo não é de graça por elemento.

Dois detalhes que mudam o resultado se forem esquecidos: o `fetchSize` do JDBC só
é honrado em transação com autoCommit desligado (sem isso o driver lê tudo e o
código *parece* streaming), e o export reativo precisa de `usingWhen` em vez de
`using`, para devolver a conexão também no cancelamento — senão o pool perde uma
conexão por download abandonado.

## 5. Composição do fan-out

`GET /stations/summary` com 8 estações, cada perna sendo uma consulta agregada
sobre 4.002.000 linhas mais 100 ms de latência simulada.

| Forma | Tempo | Ordem da resposta |
| --- | --- | --- |
| `flatMap` | 427–464 ms | ordem de conclusão |
| `flatMapSequential` | 380–482 ms | ordem da requisição |
| `concatMap` | 2736–2825 ms | ordem da requisição |
| laço bloqueante | 2781–2855 ms | ordem da requisição |

`concatMap` custa o mesmo que o laço bloqueante. Quem troca `flatMap` por
`concatMap` para "manter a ordem" paga a soma das pernas e joga fora a razão de
ter migrado; `flatMapSequential` dá as duas coisas, pagando em resultados
guardados em buffer e não em tempo.

O prefetch dos operadores tem número e está medido em teste: `flatMap` pede 256
elementos adiantado — é assim que um `flatMap` sobre mil ids acaba querendo 256
conexões de um pool de dez — e `concatMap` pede 1, sem olhar adiante.

## O que mais o projeto demonstra

### Os dois starters no classpath, e o Boot escolhendo Tomcat para WebFlux

Com `spring-boot-starter-web` presente, a autoconfiguração reativa prefere
Tomcat a Netty. A aplicação sobe, os endpoints respondem, os tipos são todos
`Mono` e `Flux` — e não há event loop nenhum, só um pool de 200 threads na
frente. Nada quebra, e toda medição sobre economia de thread fica
silenciosamente errada. `NettyWebServerConfig` declara a factory
explicitamente, e `GET /runtime` existe para que a primeira pergunta de
qualquer medição seja "qual stack está no ar".

### `DataSource` desaparece quando existe um `ConnectionFactory`

`DataSourceAutoConfiguration` traz
`@ConditionalOnMissingBean(type = "io.r2dbc.spi.ConnectionFactory")`. Numa
aplicação reativa não há `DataSource` — e, portanto, o Flyway não encontra o que
migrar e não roda. A aplicação sobe, responde, e o schema nunca foi criado. Nada
no log diz isso. `MigrationDataSourceConfig` constrói o `DataSource` à mão, a
partir das mesmas propriedades `spring.datasource.*`.

Vale lembrar o resto: não existe Flyway nem Liquibase para R2DBC. Uma aplicação
reativa que versiona schema carrega um driver bloqueante só para o startup.

### Dois transaction managers, e um `@Transactional` que escolhe errado

Com R2DBC e JDBC juntos há dois beans de `TransactionManager`.
`@Transactional` resolve por tipo: com dois candidatos e nenhum primário, a
primeira escrita anotada falha com `NoUniqueBeanDefinitionException` — em
runtime, na primeira gravação, não no startup.

Transação reativa não é transação presa a thread. O `R2dbcTransactionManager`
guarda a conexão no `Context` reativo, e um repositório que abre a própria
conexão roda fora da transação em que acredita estar: a escrita comita sozinha
e o rollback a deixa lá. `POST /readings/failing` grava e falha de propósito,
para o teste afirmar que a contagem antes e depois é a mesma.

### `ThreadLocal` não sobrevive, e o código que o usa passa na revisão

```
reativa:    {"fromReactorContext":"abc-123","fromThreadLocal":null,
             "fromMdc":"abc-123",
             "filterThread":"reactor-http-epoll-4","handlerThread":"parallel-6"}

bloqueante: {"fromReactorContext":null,"fromThreadLocal":"abc-123",
             "fromMdc":"abc-123",
             "filterThread":"http-nio-8080-exec-7","handlerThread":"http-nio-8080-exec-7"}
```

Um filtro que grava o id num `ThreadLocal` e um handler que o lê de volta são,
num fluxo reativo, duas threads. O campo volta nulo — e as mesmas duas linhas
funcionam perfeitamente na stack servlet, que é a razão de o erro atravessar a
revisão de código.

O `Context` viaja com a assinatura. E para o id chegar ao MDC — ou seja, à linha
de log — falta uma peça: um `ThreadLocalAccessor` registrado mais
`Hooks.enableAutomaticContextPropagation()`. O custo é real: cada fronteira de
operador paga um restore.

Ainda sobre posição: `contextWrite` decora **para cima**. Escrito acima do leitor,
ele não decora nada, e o código fica idêntico para quem lê de cima para baixo.

### Erro como sinal, não como pilha

Sete testes, nenhum com infraestrutura, cobrem o que um hábito de `try/catch`
produz num código reativo:

- `try/catch` em volta da montagem não pega nada, porque nada rodou ainda;
- a posição do operador decide o que ele vê — `onErrorReturn` acima do `map` que
  falha nunca é chamado;
- `doOnError` observa e reemite; não trata;
- `switchIfEmpty` avalia o argumento na montagem, então `Mono.error(new X())`
  constrói a exceção em toda chamada, inclusive nas que acharam o registro;
- `timeout` cancela a subscrição em vez de esperar por ela, o que um timeout
  bloqueante não consegue fazer;
- uma perna que falha mata o fan-out inteiro, a menos que seja contida por perna;
- e **`Retry.max` embrulha o erro final em `RetryExhaustedException`**, então um
  `@ExceptionHandler` escrito para a exceção de domínio para de casar e um 404
  vira 500. `onRetryExhaustedThrow` devolve o erro original.

### A exceção de validação tem tipo diferente em cada stack

MVC levanta `MethodArgumentNotValidException`, que estende `BindException`;
WebFlux levanta `WebExchangeBindException`, que estende
`ResponseStatusException`. Nenhuma é subclasse da outra, e um advice escrito
para a stack servlet não trata a reativa.

O modo de falhar é o que faz isso valer um parágrafo: a exceção reativa já
carrega 400, então a resposta continua sendo 400 — com o corpo padrão do Spring
em vez do contrato do projeto. O status está certo, o payload mudou de forma, e
nenhum teste que olhe só o status percebe.

### BlockHound, e o driver que ele pega

BlockHound instrumenta a JVM para que uma chamada bloqueante conhecida lance
exceção quando roda numa thread que o Reactor marca como não-bloqueante. Precisa
de `-XX:+AllowRedefinitionToAddDeleteMethods` para instalar; sem a flag o agente
falha calado e o teste que prova a detecção é um falso positivo — por isso há um
terceiro teste que verifica que a instrumentação está de pé.

E ele pega algo no próprio caminho reativo: **o r2dbc-postgresql 1.0.7 bloqueia o
event loop enquanto autentica.** O handler SASL lê de arquivo para semear o
nonce, e essa leitura é uma chamada bloqueante numa thread de loop. É breve e
acontece uma vez por conexão, então um pool quente nunca mostra; um pool que
reabastece sob carga mostra, na thread que está atendendo uma requisição. O
ponto não é que o driver esteja errado — é que "totalmente reativo" é uma
afirmação sobre a API, não uma garantia sobre o grafo de chamadas.

Isso tem um preço de arranjo: o BlockHound não pode compartilhar JVM com testes
que bloqueiam de propósito. Instalado, vale para tudo, e os endpoints de
armadilha deste projeto bloqueiam por definição. Os testes de BlockHound rodam
numa task Gradle separada (`blockHoundTest`), com JVM própria — que é como a
ferramenta deve entrar num CI.

## Modelo de dados

```
station (id, code, name, region, active)
reading (id, station_id -> station, recorded_at, temperature_c, humidity_pct)
```

Índice em `(station_id, recorded_at)`: sem ele o export seria um sort da tabela
inteira, e a medição seria sobre o sort e não sobre o streaming.

A migration semeia 8 estações com 250 leituras cada — o bastante para todo teste
ter o que ler, pouco o bastante para o start do Testcontainers não ser dominado
por isso. O volume das medições é gerado em runtime por `POST /admin/readings`:
3.200.000 linhas em 97,9 s.

## Testes

52 testes, **20 sem infraestrutura nenhuma**.

| Suíte | Testes | O que cobre |
| --- | --- | --- |
| `CompositionTest` | 4 | os três operadores, com ordem e tempo, sem banco |
| `BackpressureTest` | 6 | demanda explícita, prefetch de 256 × 1, `limitRate`, overflow e drop |
| `ErrorSignalTest` | 7 | erro como sinal, posição do operador, retry, timeout |
| `ReactiveContextTest` | 4 | `ThreadLocal` × `Context`, posição do `contextWrite`, MDC |
| `ReactiveStackIntegrationTest` | 15 | Netty, R2DBC, rollback reativo, NDJSON, armadilhas |
| `BlockingStackIntegrationTest` | 11 | Tomcat, JDBC, mesmo contrato, cursor e materializado |
| `ReactiveContextLoadsTest` | 1 | o contexto reativo sobe |
| `BlockHoundTest` | 3 | detecção, escape hatch, e a prova de que o agente instalou |
| `R2dbcDriverBlockHoundTest` | 1 | o driver bloqueando o loop ao autenticar |

As duas últimas rodam em JVM isolada:

```bash
./gradlew check          # tudo
./gradlew test           # a suíte, sem BlockHound
./gradlew blockHoundTest # só o BlockHound, em JVM própria
```

Container PostgreSQL **singleton**, em inicializador estático: o Spring
reaproveita contextos entre classes de teste, então um container por classe
seria derrubado enquanto um contexto em cache ainda apontasse para ele. Os dois
drivers apontam para o mesmo container, e a URL R2DBC é montada à mão porque o
Testcontainers só entrega a JDBC.

## Quando a stack reativa compensa

Pelo que está medido aqui:

- **Compensa** quando a concorrência que se quer atender passa do número de
  threads que se pode pagar, e o trabalho é espera em rede. É o caso 1, e o
  ganho é remover um teto, não ficar mais rápido.
- **Não muda nada** quando o gargalo é o pool de conexões, a CPU ou a consulta.
  É o caso 3, e nenhuma troca de stack conserta isso.
- **Piora** se uma única dependência bloqueante continuar no caminho — o teto cai
  para o número de loops. É o caso 2, e é o risco concreto de migrar aos poucos.
- **Cobra por elemento.** Num export de 500 mil linhas o caminho bloqueante foi
  5,8 vezes mais rápido. Fluxo com muitos elementos pequenos paga a máquina de
  operadores em cada um.
- **Cobra em ferramentas.** Sem ORM, sem lazy loading, sem Flyway nativo, stack
  trace que não conta a história, `ThreadLocal` que não funciona, e um ecossistema
  de bibliotecas onde "tem API reativa" e "não bloqueia" não são a mesma coisa.
