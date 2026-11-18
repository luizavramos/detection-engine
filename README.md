# detection-engine

Motor de detecção de fraude em tempo real. Aplicação Kafka Streams (Processor API, sem Spring) que lê `transaction-commands.v1`, mantém as features de cada conta em RocksDB, avalia as regras CEL em memória e publica a decisão em `fraud-decisions.v1` e os alertas (MEDIUM e HIGH) em `fraud-alerts.v1`. Exactly-once, static membership, saúde e métricas em `/actuator/...` pela porta 8084.

- **Features por conta:** anéis de buckets de 1 min, 1 h e 1 dia (`count`, `sum`, `max`), últimos 50 beneficiários e 10 dispositivos, em estado compacto (≤ 5 KB por conta). `avgAmount30d` e `maxAmount90d` são a referência anterior à transação atual.
- **Perfil e lista de bloqueio:** o perfil vem de uma cópia local de `customer-profile.v1` e a lista de `blocklist.v1` (tabela global); sem perfil a decisão sai `PARTIAL`.
- **Regras a quente:** o `KafkaRuleSetProvider` lê `rule-sets.v1` (eventos da gestão de regras, via outbox e Debezium), compila com o `rules-kernel` e troca o conjunto sem reiniciar; versão menor ou igual é ignorada e um conjunto que não compila mantém o anterior. O motor só fica pronto depois de ler o fim do tópico.
- **Sem duplicar nem perder:** dedup por `transactionId` (24 h) no mesmo commit do EOS, `alertId` em UUIDv5; mensagem que não desserializa vai para `transaction-commands.dlq`.
- **Backfill:** `transaction-history-backfill.v1` popula os buckets sem decidir nada.
- **Métricas:** latência fim a fim (`engine_e2e_latency_seconds`, a partir do `receivedAt`), decisões por severidade, acertos por regra, duplicatas, atrasados, DLQ, versão e idade do conjunto de regras.

Build: `./mvnw verify` (unitários, ArchUnit e um teste com Kafka real). O script do teste de aceite (H5) está em `infra/h5-acceptance.sh`, no repositório `fraud-detection-engine`.
