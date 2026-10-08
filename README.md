# Ponte RabbitMQ ⇄ Kafka com rastreabilidade por correlationId

Integração entre dois serviços **Java 21 + Spring Boot 4.1**, `intokafka` e `intorabbit`, que trocam mensagens
usando **RabbitMQ** na ida e **Apache Kafka** na volta. Cada etapa persiste no seu **PostgreSQL**.

Toda mensagem recebe um **correlationId (UUID)** no início do fluxo. Ele é propagado sem alteração por REST,
RabbitMQ, Kafka, logs e bancos de dados, então um único `grep` mostra o caminho inteiro da mensagem.

---

## Sumário

1. [Arquitetura](#arquitetura)
2. [Fluxo da mensagem passo a passo](#fluxo-da-mensagem-passo-a-passo)
3. [Stack](#stack)
4. [Estrutura do repositório](#estrutura-do-repositório)
5. [Como executar](#como-executar)
6. [API REST](#api-rest)
7. [Contrato da mensagem](#contrato-da-mensagem)
8. [Estratégia de correlationId](#estratégia-de-correlationid)
9. [RabbitMQ](#rabbitmq)
10. [Kafka](#kafka)
11. [PostgreSQL](#postgresql)
12. [Logs e rastreabilidade](#logs-e-rastreabilidade)
13. [Tratamento de erros](#tratamento-de-erros)
14. [Configuração](#configuração)
15. [Testes](#testes)
16. [Solução de problemas](#solução-de-problemas)
17. [Possíveis evoluções](#possíveis-evoluções)

---

## Arquitetura

```text
                POST /mensagens  {"mensagem": "..."}
                        │
                        ▼
┌───────────────────────────────────────────┐
│ intokafka (:8080)                         │
│  1. gera correlationId (UUID)             │──► PostgreSQL / banco intokafka
│  2. persiste ENVIO                        │      tabela mensagem (tipo=ENVIO)
│  3. publica no RabbitMQ                   │
└───────────────────────────────────────────┘
                        │  exchange mensagens.exchange
                        │  routing key mensagens.criada
                        ▼
              fila mensagens.intorabbit.queue
                        │
                        ▼
┌───────────────────────────────────────────┐
│ intorabbit (:8081)                        │
│  4. consome e preserva o correlationId    │──► PostgreSQL / banco intorabbit
│  5. persiste                              │      tabela mensagem
│  6. publica no Kafka                      │
└───────────────────────────────────────────┘
                        │  tópico mensagens.processadas
                        │  (chave = correlationId)
                        ▼
┌───────────────────────────────────────────┐
│ intokafka (consumer group intokafka-group)│
│  7. consome o retorno                     │──► PostgreSQL / banco intokafka
│  8. persiste RETORNO                      │      tabela mensagem (tipo=RETORNO)
│  9. marca o ENVIO como FINALIZADA         │      ENVIO → status FINALIZADA
└───────────────────────────────────────────┘
```

Responsabilidades de cada serviço:

| Serviço      | Entrada                    | Saída                       | Banco        |
|--------------|----------------------------|-----------------------------|--------------|
| `intokafka`  | REST `POST /mensagens`     | RabbitMQ                    | `intokafka`  |
| `intorabbit` | RabbitMQ                   | Kafka                       | `intorabbit` |
| `intokafka`  | Kafka (retorno)            | fim do fluxo                | `intokafka`  |

---

## Fluxo da mensagem passo a passo

O sistema é uma **ponte entre RabbitMQ e Kafka**. A mensagem dá uma volta completa: sai do `intokafka`, passa
pelo RabbitMQ, vira um evento Kafka no `intorabbit` e volta ao `intokafka`. Cada etapa deixa um registro no banco
do serviço que a processou, e todos os registros são ligados pelo mesmo `correlationId`.

> **Ponto de entrada único:** o fluxo sempre começa com `POST /mensagens` no `intokafka` (porta 8080).
> O `intorabbit`, o RabbitMQ e o Kafka são etapas internas e não devem ser acionados diretamente
> (veja [Entradas fora do fluxo](#entradas-fora-do-fluxo)).

| # | Serviço | O que acontece | Banco / status |
|---|---------|----------------|----------------|
| 1 | cliente | Envia `POST /mensagens` com `{"mensagem": "..."}`, somente o texto | — |
| 2 | `intokafka` | Gera o **correlationId** (UUID) e a `dataCadastro` | — |
| 3 | `intokafka` | Grava a linha de **ENVIO** | `intokafka`: `ENVIO` / `AGUARDANDO_PUBLICACAO` |
| 4 | `intokafka` | Publica `{correlationId, mensagem, dataCadastro}` em `mensagens.exchange`, aguarda a confirmação do broker e responde **202** com o correlationId | `intokafka`: `ENVIO` / `PUBLICADA_RABBITMQ` |
| 5 | `intorabbit` | Consome de `mensagens.intorabbit.queue` mantendo o **mesmo** correlationId e grava | `intorabbit`: `RECEBIDA_RABBITMQ` |
| 6 | `intorabbit` | Publica o mesmo evento no tópico `mensagens.processadas` (chave = correlationId) | `intorabbit`: `PUBLICADA_KAFKA` |
| 7 | `intokafka` | Consome o retorno (consumer group `intokafka-group`) e grava a linha de **RETORNO** | `intokafka`: `RETORNO` / `RETORNO_RECEBIDO_KAFKA` |
| 8 | `intokafka` | Marca o ENVIO original como concluído e loga `Fluxo finalizado` | `intokafka`: `ENVIO` / `FINALIZADA` |

O fluxo inteiro leva menos de 1 segundo.

### Por que a mensagem aparece nos dois bancos

É o comportamento esperado. Os serviços são independentes e cada um registra no **próprio** banco a parte do
fluxo que processou. Nenhum serviço lê o banco do outro; a comunicação acontece só pelas mensagens. Ao final de
um fluxo completo existem:

- **2 linhas** no banco `intokafka`: `ENVIO` (status `FINALIZADA`) e `RETORNO`;
- **1 linha** no banco `intorabbit` (status `PUBLICADA_KAFKA`).

Esses registros mostram onde uma mensagem parou:

| Situação nos bancos | Onde está o problema |
|---|---|
| Só `ENVIO` no `intokafka`, nada no `intorabbit` | a mensagem ficou no RabbitMQ, ou o `intorabbit` está fora do ar |
| `ENVIO` com `FALHA_PUBLICACAO_RABBITMQ` | o RabbitMQ não confirmou a publicação (o cliente recebeu `503`) |
| Registro no `intorabbit` com `FALHA_PUBLICACAO_KAFKA` | o Kafka estava indisponível; após as retentativas a mensagem vai para a DLQ |
| `intorabbit` com `PUBLICADA_KAFKA`, mas sem `RETORNO` no `intokafka` | o consumer Kafka do `intokafka` falhou (verifique a DLT) |
| `ENVIO` = `FINALIZADA` + `RETORNO` presente | fluxo concluído |

### Como validar um teste

1. Envie a mensagem e guarde o `correlationId` da resposta:
   ```bash
   curl -i -X POST http://localhost:8080/mensagens \
     -H "Content-Type: application/json" -d '{"mensagem": "Teste de integração"}'
   ```
2. **API:** `GET http://localhost:8080/mensagens/<correlationId>` deve retornar o `ENVIO` com status `FINALIZADA` e o
   `RETORNO`.
3. **Logs:** `docker compose logs intokafka intorabbit | grep <correlationId>` mostra todas as etapas em ordem.
4. **Bancos** (DBeaver ou `psql` em `localhost:5433`): 2 linhas em `intokafka.mensagem` e 1 linha em
   `intorabbit.mensagem` (ver [Consultar pelo correlationId](#consultar-pelo-correlationid)).
5. **Interfaces:** a mensagem aparece no tópico `mensagens.processadas` do Kafka UI (http://localhost:8085), com a
   chave e o header `X-Correlation-Id`. Na Management UI do RabbitMQ (http://localhost:15672) a fila
   `mensagens.intorabbit.queue` fica vazia, o que indica que a mensagem foi consumida.

### Entradas fora do fluxo

| O que foi feito | Resultado |
|---|---|
| `POST` no `intorabbit` (`localhost:8081/mensagens`) | **404**: o `intorabbit` não tem endpoint REST, só o Actuator (`/actuator/health`) |
| Publicar direto no RabbitMQ **com** `correlationId` | A mensagem é processada a partir da etapa 5 e o correlationId informado é mantido. Como não passou pelo REST, o `intokafka` grava só o `RETORNO` e loga `WARN Registro de ENVIO não encontrado para este correlationId` |
| Publicar direto no RabbitMQ **sem** `correlationId` | Rejeitada: nenhum ID é gerado no meio do fluxo. A mensagem é logada em `ERROR` e enviada para a DLQ `mensagens.intorabbit.dlq`, sem gravar nada |
| Publicar direto no Kafka com JSON inválido | Logada em `ERROR` e enviada para a DLT `mensagens.processadas.DLT` |

---

## Stack

| Componente          | Versão / imagem                    |
|---------------------|------------------------------------|
| Java                | 21                                 |
| Spring Boot         | 4.1.1 (Spring Framework 7, Jackson 3) |
| Spring AMQP         | 4.1 (`spring-boot-starter-amqp`)   |
| Spring for Kafka    | 4.1 (`spring-boot-starter-kafka`)  |
| Spring Data JPA     | Hibernate 7                        |
| PostgreSQL          | `postgres:17-alpine`               |
| RabbitMQ            | `rabbitmq:4.1-management-alpine`   |
| Apache Kafka        | `apache/kafka:4.1.0` (KRaft, sem ZooKeeper) |
| Kafka UI            | `provectuslabs/kafka-ui:latest`    |
| Logs                | SLF4J + Logback com MDC            |

Dependências adicionadas aos dois `pom.xml`: `spring-boot-starter-validation`, `spring-boot-starter-actuator`,
`spring-boot-starter-data-jpa`, `spring-boot-starter-amqp`, `spring-boot-starter-kafka` e o driver `postgresql`.

---

## Estrutura do repositório

```text
kafkaandrabbit/
├── docker-compose.yml
├── README.md
├── docker/
│   ├── postgres/init/01-create-databases.sql   # cria os bancos intokafka e intorabbit
│   └── certs/                                  # CAs extras opcionais para o build (ver Solução de problemas)
├── intokafka/intokafka/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/br/com/joshua/intokafka/
│       ├── config/        RabbitMQConfig, KafkaConfig, *Properties, ClockConfig
│       ├── consumer/      MensagemKafkaConsumer
│       ├── controller/    MensagemController
│       ├── correlation/   CorrelationId, CorrelationIdFilter
│       ├── dto/           MensagemRequest, MensagemResponse, MensagemEvent
│       ├── entity/        Mensagem, TipoMensagem, StatusMensagem
│       ├── exception/     GlobalExceptionHandler, MensagemPublicacaoException, EventoInvalidoException
│       ├── producer/      MensagemRabbitProducer
│       ├── repository/    MensagemRepository
│       └── service/       MensagemService, MensagemRetornoService
└── intorabbit/intorabbit/
    ├── Dockerfile
    ├── pom.xml
    └── src/main/java/br/com/joshua/intorabbit/
        ├── config/        RabbitMQConfig, KafkaConfig, *Properties, ClockConfig
        ├── consumer/      MensagemRabbitConsumer
        ├── correlation/   CorrelationId
        ├── dto/           MensagemEvent
        ├── entity/        Mensagem, StatusMensagem
        ├── exception/     MensagemPublicacaoException, EventoInvalidoException
        ├── producer/      MensagemKafkaProducer
        ├── repository/    MensagemRepository
        └── service/       MensagemService
```

Controllers, producers e consumers não contêm regra de negócio. Eles tratam do protocolo (HTTP, AMQP, Kafka)
e do MDC, e delegam o processamento aos **services**.

---

## Como executar

### Pré-requisitos

- Docker com Docker Compose v2.
- Portas livres no host: `8080`, `8081`, `8085`, `5433`, `5672`, `15672`, `29092`.

### Subir tudo

```bash
docker compose up --build
```

Ordem de inicialização, garantida por `healthcheck` + `depends_on: condition: service_healthy`:

```text
postgres, rabbitmq, kafka  (healthy)
        ├──► kafka-ui
        └──► intorabbit     (healthy: /actuator/health/readiness)
                └──► intokafka (healthy)  →  POST /mensagens disponível
```

| Healthcheck  | Como é verificado                                                         |
|--------------|---------------------------------------------------------------------------|
| `postgres`   | `psql` conecta via TCP nos **dois** bancos (o script de init já rodou)    |
| `rabbitmq`   | `rabbitmq-diagnostics check_port_connectivity`                             |
| `kafka`      | `kafka-broker-api-versions.sh --bootstrap-server kafka:9092`              |
| apps         | `GET /actuator/health/readiness` = `UP`                                   |

### Acessos

| Serviço      | Endereço                |
|--------------|-------------------------|
| RabbitMQ     | localhost:15672         |
| Kafka UI     | localhost:8085          |
| Kafka Broker | localhost:29092 (TCP)   |

Todos os endereços expostos no host:

| Recurso                | Endereço                                   | Credenciais               |
|------------------------|--------------------------------------------|---------------------------|
| API intokafka          | http://localhost:8080/mensagens            | —                         |
| Health intokafka       | http://localhost:8080/actuator/health      | —                         |
| Health intorabbit      | http://localhost:8081/actuator/health      | —                         |
| RabbitMQ Management UI | http://localhost:15672                     | `integracao` / `integracao` |
| Kafka UI               | http://localhost:8085                      | —                         |
| Kafka Broker           | `localhost:29092` (TCP)                    | —                         |
| PostgreSQL             | `localhost:5433` (TCP)                     | `postgres` / `postgres`   |

O PostgreSQL é publicado na porta **5433** do host para não conflitar com um PostgreSQL instalado localmente
(porta 5432). Dentro da rede Docker as aplicações continuam usando `postgres:5432`. Para usar outra porta no
host, defina `POSTGRES_HOST_PORT` (ex.: no `.env`).

O **Kafka UI** (cluster `integracao`) permite navegar pelos tópicos `mensagens.processadas` e
`mensagens.processadas.DLT`, ver as mensagens com chave e headers (`X-Correlation-Id`) e acompanhar o lag do
consumer group `intokafka-group`.

### Rede

Todos os containers estão na rede `integracao`. Entre containers a comunicação usa **apenas os nomes dos serviços**:
`postgres:5432`, `rabbitmq:5672` e `kafka:9092`. O Kafka anuncia dois listeners: `kafka:9092` para os containers e
`localhost:29092` para ferramentas no host.

### Parar

```bash
docker compose down        # mantém os dados do PostgreSQL
docker compose down -v     # remove também o volume (os bancos são recriados no próximo up)
```

---

## API REST

### `POST /mensagens`

O cliente envia **somente** `mensagem`. `id`, `correlationId` e `dataCadastro` são gerados pela aplicação;
enviar qualquer outro campo resulta em `400 Bad Request`.

```bash
curl -i -X POST http://localhost:8080/mensagens \
  -H "Content-Type: application/json" \
  -d '{"mensagem": "Teste de integração"}'
```

PowerShell:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/mensagens `
  -ContentType "application/json; charset=utf-8" `
  -Body '{"mensagem": "Teste de integração"}'
```

Resposta `202 Accepted`. O processamento continua de forma assíncrona (RabbitMQ → intorabbit → Kafka).

```http
HTTP/1.1 202
X-Correlation-Id: 568cb37f-41e2-4168-9534-32dd5d392382
Location: http://localhost:8080/mensagens/568cb37f-41e2-4168-9534-32dd5d392382
Content-Type: application/json
```

```json
{
  "id": 1,
  "correlationId": "568cb37f-41e2-4168-9534-32dd5d392382",
  "mensagem": "Teste de integração",
  "dataCadastro": "2026-10-07T21:56:19.275677",
  "tipo": "ENVIO",
  "status": "PUBLICADA_RABBITMQ",
  "dataAtualizacao": "2026-10-07T21:56:19.414136"
}
```

| Status | Quando                                                                    |
|--------|---------------------------------------------------------------------------|
| `202`  | Mensagem persistida e confirmada pelo RabbitMQ                            |
| `400`  | `mensagem` vazia, maior que 4000 caracteres, JSON inválido ou campos extras |
| `503`  | Mensagem persistida, mas o RabbitMQ não confirmou a publicação            |

Os erros seguem o padrão **Problem Details (RFC 9457)** e sempre trazem o `correlationId`:

```json
{
  "type": "about:blank",
  "title": "Bad Request",
  "status": 400,
  "detail": "Corpo da requisição inválido. Envie somente o campo 'mensagem'; id, correlationId e dataCadastro são gerados pela aplicação.",
  "instance": "/mensagens",
  "correlationId": "f2191a94-1e68-475f-902d-246cef81e09d"
}
```

### `GET /mensagens/{correlationId}`

Endpoint auxiliar que lista os registros do banco `intokafka` para um correlationId. Com o fluxo concluído,
retorna o `ENVIO` (status `FINALIZADA`) e o `RETORNO`.

```bash
curl http://localhost:8080/mensagens/568cb37f-41e2-4168-9534-32dd5d392382
```

---

## Contrato da mensagem

O DTO REST e o evento de integração são **separados**:

| Classe            | Onde                      | Campos                                     |
|-------------------|---------------------------|--------------------------------------------|
| `MensagemRequest` | Entrada REST              | `mensagem`                                 |
| `MensagemEvent`   | RabbitMQ e Kafka          | `correlationId`, `mensagem`, `dataCadastro` |
| `MensagemResponse`| Saída REST                | dados do registro persistido               |

```java
public record MensagemEvent(String correlationId, String mensagem, LocalDateTime dataCadastro) {}
```

JSON trafegado no RabbitMQ e no Kafka:

```json
{
  "correlationId": "568cb37f-41e2-4168-9534-32dd5d392382",
  "mensagem": "Teste de integração",
  "dataCadastro": "2026-10-07T21:56:19.275677"
}
```

Cada serviço tem a sua própria cópia de `MensagemEvent`, sem dependência de código compartilhado. O contrato é o
JSON:

- **RabbitMQ:** o header `__TypeId__` usa o identificador lógico `MensagemEvent`, e não o nome da classe Java.
  Cada lado mapeia esse identificador para a sua classe.
- **Kafka:** o produtor não envia type headers (`spring.json.add.type.headers=false`). O consumidor desserializa
  para o tipo padrão configurado (`spring.json.value.default.type`).

`dataCadastro` é gerada com precisão de **microssegundos**, a mesma do `timestamp` do PostgreSQL. Assim o valor é
idêntico no evento e nos dois bancos.

---

## Estratégia de correlationId

1. **Geração (uma única vez):** o `CorrelationIdFilter` do `intokafka` executa
   `UUID.randomUUID().toString()` no início de cada requisição HTTP. O valor é:
   - colocado no **MDC**, então todos os logs da requisição o contêm;
   - disponibilizado ao controller como atributo da requisição;
   - devolvido ao cliente no header `X-Correlation-Id` e no corpo da resposta.
2. **Persistência:** gravado na coluna `correlation_id` (indexada) do banco `intokafka`.
3. **RabbitMQ:** enviado em três lugares: corpo JSON, propriedade AMQP `correlation_id` e header `X-Correlation-Id`.
4. **intorabbit:** lê o valor do corpo (com a propriedade AMQP como fallback), coloca no MDC, persiste e repassa
   **o mesmo valor**. Este serviço não tem nenhum código que gere UUID.
5. **Kafka:** enviado no corpo JSON, como **chave do registro** e no header `X-Correlation-Id`. Usar a chave
   mantém as mensagens do mesmo correlationId na mesma partição e permite logar o ID mesmo quando o JSON é inválido.
6. **intokafka (retorno):** lê o valor do corpo (com a chave como fallback), coloca no MDC e persiste o `RETORNO`.

Um evento que chegue **sem** correlationId é tratado como inválido e enviado para a DLQ/DLT. Nenhum ID novo é
gerado no meio do fluxo.

O MDC é sempre aberto com `try-with-resources` (`MDC.putCloseable`), então é limpo ao final de cada
requisição ou mensagem e nunca vaza entre threads do pool.

---

## RabbitMQ

| Item                    | Valor                         | Variável de ambiente       |
|-------------------------|-------------------------------|----------------------------|
| Exchange (direct)       | `mensagens.exchange`          | `RABBITMQ_EXCHANGE`        |
| Queue                   | `mensagens.intorabbit.queue`  | `RABBITMQ_QUEUE`           |
| Routing key             | `mensagens.criada`            | `RABBITMQ_ROUTING_KEY`     |
| Dead-letter exchange    | `mensagens.dlx`               | `RABBITMQ_DLX`             |
| Dead-letter queue       | `mensagens.intorabbit.dlq`    | `RABBITMQ_DLQ`             |
| Dead-letter routing key | `mensagens.criada.dlq`        | `RABBITMQ_DLQ_ROUTING_KEY` |

- Exchanges e filas são **duráveis**. A fila principal tem `x-dead-letter-exchange` apontando para a DLX.
- A topologia é declarada pelos **dois** serviços (as declarações AMQP são idempotentes), então a ordem de
  inicialização não faz mensagens se perderem por falta de rota.
- **Producer (intokafka):** usa **publisher confirms** (`publisher-confirm-type: simple` +
  `waitForConfirmsOrDie`). O `202` só é devolvido depois que o broker confirma a mensagem. Com `mandatory=true`,
  mensagens sem rota retornam e são logadas como erro.
- **Consumer (intorabbit):** usa o `JacksonJsonMessageConverter` (Jackson 3), com 3 tentativas (backoff
  exponencial de 2s a 10s). Esgotadas as tentativas, a mensagem é rejeitada e segue para a DLQ.

Na Management UI (http://localhost:15672), em **Queues** é possível acompanhar as filas e usar *Get messages*
para ver o payload e as propriedades `correlation_id` / `X-Correlation-Id`.

---

## Kafka

| Item              | Valor                          | Variável de ambiente     |
|-------------------|--------------------------------|--------------------------|
| Topic             | `mensagens.processadas`        | `KAFKA_TOPIC`            |
| Dead Letter Topic | `mensagens.processadas.DLT`    | `KAFKA_DLT`              |
| Consumer group    | `intokafka-group`              | `KAFKA_CONSUMER_GROUP`   |
| Partições         | 3                              | `KAFKA_TOPIC_PARTITIONS` |
| Chave             | `correlationId`                | —                        |

- Os tópicos são criados pelo `KafkaAdmin` a partir de beans `NewTopic`.
- **Producer (intorabbit):** usa `JacksonJsonSerializer`, `acks=all` e idempotência habilitada. O envio é
  síncrono (`send().get(timeout)`), então uma falha volta para o consumer RabbitMQ e a mensagem é retentada.
- **Consumer (intokafka):** usa `ErrorHandlingDeserializer` + `JacksonJsonDeserializer` e
  `auto-offset-reset=earliest`. O `DefaultErrorHandler` faz 3 novas tentativas com intervalo de 2s antes de
  enviar o registro para a DLT.

Comandos úteis (o prefixo `MSYS_NO_PATHCONV=1` só é necessário no Git Bash do Windows):

```bash
# Ler o tópico com chave e headers
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic mensagens.processadas --from-beginning \
  --property print.key=true --property print.headers=true

# Ver offsets e lag do consumer group
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --describe --group intokafka-group
```

---

## PostgreSQL

Uma instância (`postgres`) com **dois bancos separados**, criados por `docker/postgres/init/01-create-databases.sql`:

| Aplicação    | Banco        | URL (dentro da rede Docker)                    |
|--------------|--------------|------------------------------------------------|
| `intokafka`  | `intokafka`  | `jdbc:postgresql://postgres:5432/intokafka`    |
| `intorabbit` | `intorabbit` | `jdbc:postgresql://postgres:5432/intorabbit`   |

As tabelas são criadas pelo Hibernate (`ddl-auto: update`) a partir das entidades JPA.

**`intokafka.mensagem`**: um registro por etapa (`ENVIO` e `RETORNO`).

| Coluna             | Tipo          | Observação                                                  |
|--------------------|---------------|-------------------------------------------------------------|
| `id`               | bigint (PK)   | chave local                                                 |
| `correlation_id`   | varchar(36)   | índice `idx_mensagem_correlation_id`                        |
| `mensagem`         | varchar(4000) |                                                             |
| `data_cadastro`    | timestamp     | gerada no início do fluxo                                   |
| `tipo`             | varchar(20)   | `ENVIO` / `RETORNO`; único por correlationId (`uk_mensagem_correlation_id_tipo`) |
| `status`           | varchar(40)   | `AGUARDANDO_PUBLICACAO`, `PUBLICADA_RABBITMQ`, `FALHA_PUBLICACAO_RABBITMQ`, `FINALIZADA`, `RETORNO_RECEBIDO_KAFKA` |
| `data_atualizacao` | timestamp     |                                                             |

**`intorabbit.mensagem`**: um registro por correlationId.

| Coluna             | Tipo          | Observação                                                  |
|--------------------|---------------|-------------------------------------------------------------|
| `id`               | bigint (PK)   | chave local                                                 |
| `correlation_id`   | varchar(36)   | índice **único** `idx_mensagem_correlation_id`              |
| `mensagem`         | varchar(4000) |                                                             |
| `data_cadastro`    | timestamp     | preservada do evento original                               |
| `data_recebimento` | timestamp     | quando o intorabbit recebeu a mensagem                      |
| `status`           | varchar(40)   | `RECEBIDA_RABBITMQ`, `PUBLICADA_KAFKA`, `FALHA_PUBLICACAO_KAFKA` |
| `data_atualizacao` | timestamp     |                                                             |

### Consultar pelo correlationId

```bash
CID=568cb37f-41e2-4168-9534-32dd5d392382

docker exec -it postgres psql -U postgres -d intokafka \
  -c "SELECT * FROM mensagem WHERE correlation_id = '$CID' ORDER BY id;"

docker exec -it postgres psql -U postgres -d intorabbit \
  -c "SELECT * FROM mensagem WHERE correlation_id = '$CID';"
```

Resultado de um fluxo completo:

```text
intokafka
 id | correlation_id                       | mensagem            | tipo    | status
----+--------------------------------------+---------------------+---------+------------------------
  1 | 568cb37f-41e2-4168-9534-32dd5d392382 | Teste de integração | ENVIO   | FINALIZADA
  2 | 568cb37f-41e2-4168-9534-32dd5d392382 | Teste de integração | RETORNO | RETORNO_RECEBIDO_KAFKA

intorabbit
 id | correlation_id                       | mensagem            | status
----+--------------------------------------+---------------------+-----------------
  1 | 568cb37f-41e2-4168-9534-32dd5d392382 | Teste de integração | PUBLICADA_KAFKA
```

Também é possível conectar de qualquer cliente (DBeaver, pgAdmin etc.) em `localhost:5433`:

```bash
psql -h localhost -p 5433 -U postgres -d intokafka \
  -c "SELECT * FROM mensagem WHERE correlation_id = '$CID' ORDER BY id;"
```

---

## Logs e rastreabilidade

Os logs usam SLF4J + Logback (`logback-spring.xml`), sem `System.out.println`. O pattern inclui o serviço e o
`correlationId` do MDC:

```text
%d %-5level [INTOKAFKA] [correlationId=%X{correlationId:--}] [%thread] %logger : %msg
```

Para rastrear uma mensagem nos dois serviços:

```bash
docker compose logs intokafka intorabbit | grep 568cb37f-41e2-4168-9534-32dd5d392382
```

Saída real de um fluxo (threads e nomes de logger abreviados):

```text
21:56:19.275 INFO  [INTOKAFKA]  [correlationId=568cb37f-...] MensagemController     : Requisição REST recebida [POST /mensagens]
21:56:19.331 INFO  [INTOKAFKA]  [correlationId=568cb37f-...] MensagemService        : Mensagem persistida [banco=intokafka, tabela=mensagem, id=1, tipo=ENVIO, status=AGUARDANDO_PUBLICACAO]
21:56:19.331 INFO  [INTOKAFKA]  [correlationId=568cb37f-...] MensagemRabbitProducer : Publicando mensagem no RabbitMQ [exchange=mensagens.exchange, routingKey=mensagens.criada]
21:56:19.414 INFO  [INTOKAFKA]  [correlationId=568cb37f-...] MensagemRabbitProducer : Mensagem publicada no RabbitMQ e confirmada pelo broker
21:56:19.448 INFO  [INTORABBIT] [correlationId=568cb37f-...] MensagemRabbitConsumer : Mensagem recebida do RabbitMQ [queue=mensagens.intorabbit.queue]
21:56:19.604 INFO  [INTORABBIT] [correlationId=568cb37f-...] MensagemService        : Mensagem persistida [banco=intorabbit, tabela=mensagem, id=1, status=RECEBIDA_RABBITMQ]
21:56:19.605 INFO  [INTORABBIT] [correlationId=568cb37f-...] MensagemKafkaProducer  : Publicando mensagem no Kafka [topic=mensagens.processadas]
21:56:19.819 INFO  [INTORABBIT] [correlationId=568cb37f-...] MensagemKafkaProducer  : Mensagem publicada no Kafka [topic=mensagens.processadas, partition=0, offset=0]
21:56:19.828 INFO  [INTOKAFKA]  [correlationId=568cb37f-...] MensagemKafkaConsumer  : Mensagem recebida do Kafka [topic=mensagens.processadas, partition=0, offset=0]
21:56:19.870 INFO  [INTOKAFKA]  [correlationId=568cb37f-...] MensagemRetornoService : Retorno persistido [banco=intokafka, tabela=mensagem, id=2, tipo=RETORNO, status=RETORNO_RECEBIDO_KAFKA]
21:56:19.874 INFO  [INTOKAFKA]  [correlationId=568cb37f-...] MensagemRetornoService : Fluxo finalizado [REST -> PostgreSQL -> RabbitMQ -> intorabbit -> Kafka -> intokafka]
```

---

## Tratamento de erros

Toda falha é logada em `ERROR` com **correlationId, origem, destino e erro**. Nenhuma exceção é engolida: ela é
propagada para quem decide entre retentar, enviar para DLQ/DLT ou responder ao cliente.

| Cenário                                  | Comportamento                                                                                   |
|------------------------------------------|--------------------------------------------------------------------------------------------------|
| Body inválido / campos extras            | `400` Problem Details com correlationId                                                         |
| RabbitMQ não confirma a publicação       | `ERROR` no producer, registro com status `FALHA_PUBLICACAO_RABBITMQ`, resposta `503`            |
| Mensagem sem rota no RabbitMQ            | `ERROR` pelo *returns callback* (`mandatory=true`)                                              |
| Kafka indisponível (no intorabbit)       | `ERROR` no producer, status `FALHA_PUBLICACAO_KAFKA`, 3 tentativas, depois **DLQ** `mensagens.intorabbit.dlq` |
| Reentrega do RabbitMQ                    | idempotente: reaproveita o registro existente e não republica o que já foi publicado            |
| JSON inválido / sem correlationId no Kafka | não é retentado: `ERROR` e envio direto para a **DLT** `mensagens.processadas.DLT`            |
| Falha ao persistir o retorno             | 3 tentativas (2s), depois **DLT**                                                               |
| Retorno duplicado no Kafka               | idempotente: ignorado com `WARN`                                                                |

Exemplo real com o Kafka parado:

```text
ERROR [INTORABBIT] [correlationId=f221313a-...] MensagemKafkaProducer  : Falha ao publicar mensagem no Kafka [origem=intorabbit, destino=mensagens.processadas, erro=org.apache.kafka.common.errors.TimeoutException: ...]
ERROR [INTORABBIT] [correlationId=f221313a-...] MensagemRabbitConsumer : Falha ao processar mensagem consumida do RabbitMQ [origem=mensagens.intorabbit.queue, destino=intorabbit, erro=...]
WARN  [INTORABBIT] [correlationId=f221313a-...] MensagemService        : Mensagem já persistida anteriormente (reentrega do RabbitMQ) [id=2, status=FALHA_PUBLICACAO_KAFKA]
...
ERROR [INTORABBIT] [correlationId=f221313a-...] RabbitMQConfig         : Tentativas esgotadas ao consumir mensagem do RabbitMQ, enviando para a DLQ [origem=mensagens.intorabbit.queue, destino=mensagens.intorabbit.dlq, erro=...]
```

Para reprocessar, use a Management UI: a fila `mensagens.intorabbit.dlq` permite *Move messages* de volta para
`mensagens.intorabbit.queue` (plugin shovel) ou *Get messages* para inspeção.

---

## Configuração

Toda a configuração fica nos `application.yml`. Nomes de filas e tópicos ficam centralizados em `app.rabbitmq.*` /
`app.kafka.*` (classes `RabbitMQProperties` e `KafkaTopicProperties`, validadas na inicialização). O código Java
não contém credenciais nem endereços de infraestrutura.

Os valores padrão dos `application.yml` apontam para `localhost`, o que permite rodar uma aplicação pela IDE com a
infraestrutura no Docker. O `docker-compose.yml` sobrescreve tudo por variáveis de ambiente com os nomes dos
serviços.

| Variável                  | Padrão (local)       | Docker                 |
|---------------------------|----------------------|------------------------|
| `SERVER_PORT`             | 8080 / 8081          | 8080 / 8081            |
| `DB_HOST` / `DB_PORT`     | `localhost` / `5433` | `postgres` / `5432`    |
| `DB_NAME`                 | `intokafka` / `intorabbit` | idem             |
| `DB_USERNAME` / `DB_PASSWORD` | `postgres`       | `${POSTGRES_USER}` / `${POSTGRES_PASSWORD}` |
| `RABBITMQ_HOST` / `RABBITMQ_PORT` | `localhost` / `5672` | `rabbitmq` / `5672` |
| `RABBITMQ_USERNAME` / `RABBITMQ_PASSWORD` | `guest` | `${RABBITMQ_USER}` / `${RABBITMQ_PASSWORD}` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092`    | `kafka:9092`           |
| `JPA_DDL_AUTO`            | `update`             | `update`               |
| `TZ`                      | sistema              | `America/Sao_Paulo`    |

As credenciais do compose podem ser trocadas sem editar arquivos, com um `.env` na raiz:

```dotenv
POSTGRES_HOST_PORT=5433
POSTGRES_USER=postgres
POSTGRES_PASSWORD=uma-senha-forte
RABBITMQ_USER=integracao
RABBITMQ_PASSWORD=outra-senha-forte
```

### Executar uma aplicação fora do Docker

```bash
docker compose up -d postgres rabbitmq kafka
cd intokafka/intokafka && ./mvnw spring-boot:run      # RabbitMQ local: use RABBITMQ_USERNAME/PASSWORD=integracao
```

---

## Testes

```bash
cd intokafka/intokafka  && ./mvnw test
cd intorabbit/intorabbit && ./mvnw test
```

| Teste                         | O que garante                                                                      |
|-------------------------------|-------------------------------------------------------------------------------------|
| `MensagemControllerTest`      | UUID gerado por requisição, `202` + header, rejeição de `correlationId` enviado pelo cliente e de mensagem em branco |
| `MensagemServiceTest` (intokafka) | evento publicado com o mesmo correlationId e `dataCadastro`; status de falha + exceção propagada |
| `MensagemRetornoServiceTest`  | retorno persistido com o correlationId original, idempotência, evento sem ID rejeitado (nenhum ID novo é gerado) |
| `MensagemServiceTest` (intorabbit) | republica o mesmo evento, não republica o que já foi publicado, falha propagada, evento inválido rejeitado |

Os testes não dependem de infraestrutura. O fluxo ponta a ponta é validado com `docker compose up`.

---

## Solução de problemas

**`PKIX path building failed` durante o `docker compose up --build`**
Algum antivírus (ex.: Norton Web/Mail Shield) ou proxy corporativo está inspecionando HTTPS, e o Maven dentro do
container não confia na CA dele. Exporte a CA raiz em formato PEM (`.crt` ou `.pem`) para `docker/certs/`. O
build importa automaticamente qualquer certificado desse diretório no truststore do JDK (somente no estágio de
build). Os arquivos de certificado são ignorados pelo Git. Exemplo no PowerShell:

```powershell
$c = Get-ChildItem Cert:\LocalMachine\Root | Where-Object Subject -like "*NOME DA CA*" | Select-Object -First 1
"-----BEGIN CERTIFICATE-----`n" + [Convert]::ToBase64String($c.RawData, 'InsertLineBreaks') + "`n-----END CERTIFICATE-----" |
  Set-Content -Encoding ascii docker\certs\ca-corporativa.crt
```

**`400` com `Invalid UTF-8` ao testar pelo terminal do Windows**
O terminal enviou o corpo em outra codificação. Use o exemplo em PowerShell acima, um arquivo salvo em UTF-8
(`curl --data-binary @body.json`) ou uma mensagem sem acentos.

**`404` com `"instance": "/mensagens%20"`**
A URL foi enviada com um espaço no final (`%20`), algo comum ao colar no Postman ou Insomnia. Remova o espaço:
`http://localhost:8080/mensagens`.

**Os bancos não aparecem no DBeaver**
Verifique se a conexão usa a porta **5433**. A 5432 é a de um PostgreSQL local, se houver. Na aba
**PostgreSQL** da conexão, marque **"Show all databases"** para listar `intokafka` e `intorabbit` juntos. As tabelas
ficam em `<banco> → Schemas → public → Tables → mensagem`.

**Os bancos não foram criados**
O script de init só roda com o volume vazio. Execute `docker compose down -v` e suba novamente.

**Porta já em uso**
Altere o mapeamento `host:container` no `docker-compose.yml` (ex.: `"18080:8080"`).

---

## Possíveis evoluções

- **Transactional Outbox** no `intokafka`, para garantir atomicidade entre a gravação no banco e a publicação no
  RabbitMQ sem depender do `503` + retentativa do cliente.
- **Flyway** para versionar o schema em vez de `ddl-auto: update`.
- **Micrometer Tracing / OpenTelemetry**, propagando também `traceId`/`spanId` e exportando para Jaeger ou Tempo.
- **Testcontainers** para testes de integração automatizados do fluxo completo.
- Logs em **JSON** (Logback encoder estruturado) para ingestão em ELK, Loki etc.
