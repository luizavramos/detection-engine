# detection-engine

Motor de detecção de fraude em tempo real. Aplicação Kafka Streams (Processor API, sem Spring) que lê `transaction-commands.v1`, mantém as features de cada conta em RocksDB, avalia as regras CEL em memória e publica a decisão em `fraud-decisions.v1` e os alertas (MEDIUM e HIGH) em `fraud-alerts.v1`. Exactly-once, static membership, saúde e métricas em `/actuator/...` pela porta 8084.

- **Features por conta:** anéis de buckets de 1 min, 1 h e 1 dia (`count`, `sum`, `max`), últimos 50 beneficiários e 10 dispositivos, em estado compacto (≤ 5 KB por conta). `avgAmount30d` e `maxAmount90d` são a referência anterior à transação atual.
- **Perfil e lista de bloqueio:** o perfil vem de uma cópia local de `customer-profile.v1` e a lista de `blocklist.v1` (tabela global); sem perfil a decisão sai `PARTIAL`.
- **Regras a quente:** o `KafkaRuleSetProvider` lê `rule-sets.v1` (eventos da gestão de regras, via outbox e Debezium), compila com o `rules-kernel` e troca o conjunto sem reiniciar; versão menor ou igual é ignorada e um conjunto que não compila mantém o anterior. O motor só fica pronto depois de ler o fim do tópico.
- **Sem duplicar nem perder:** dedup por `transactionId` (24 h) no mesmo commit do EOS, `alertId` em UUIDv5; mensagem que não desserializa vai para `transaction-commands.dlq`.
- **Backfill:** `transaction-history-backfill.v1` popula os buckets sem decidir nada.
- **Métricas:** latência fim a fim (`engine_e2e_latency_seconds`, a partir do `receivedAt`), decisões por severidade, acertos por regra, duplicatas, atrasados, DLQ, versão e idade do conjunto de regras.

## Configuração

Lida do ambiente (`StreamsSettings`, do `platform-streams`):

| Variável | Para quê |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | brokers |
| `HOSTNAME` | identifica a instância (static membership); no compose cada réplica tem o seu |
| `HTTP_PORT` | saúde e métricas (8084 no compose) |
| `STATE_DIR` | diretório do estado local (RocksDB); precisa sobreviver a reinício para a instância voltar rápido |
| `REPLICATION_FACTOR` | fator de replicação dos tópicos internos do Streams |
| `STANDBY_REPLICAS` | réplicas de reserva do estado, para a troca de instância ser rápida |

## Como rodar

Precisa de Java 21, Docker (um dos testes sobe Kafka real) e dos artefatos `motor-fraude-parent`, `contracts`, `platform-commons`, `platform-streams` e `rules-kernel` instalados (`./mvnw install` em cada um, nessa ordem).

```bash
./mvnw verify                  # unitários, ArchUnit e um teste com Kafka real
./mvnw -DskipTests package     # gera target/detection-engine-0.1.0.jar
```

O motor só fica pronto (`/actuator/health/readiness`) depois de ler o conjunto de regras de `rule-sets.v1`; sem nenhum conjunto publicado ele continua não pronto. Para subi-lo com Kafka, Debezium, Postgres e observabilidade, use o `docker-compose.yml` do repositório https://github.com/luizavramos/fraud-detection-engine (perfil `app`). Lá também estão os roteiros que provam o comportamento no compose: `infra/hot-rules-acceptance.sh` (regra nova valendo em segundos, sem reiniciar) e `infra/chaos-engine.sh` (derrubar uma instância).
