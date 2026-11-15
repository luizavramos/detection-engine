# detection-engine

Motor de detecção de fraude em tempo real. Aplicação Kafka Streams (Processor API, sem Spring) que lê `transaction-commands.v1`, mantém as features de cada conta em RocksDB, avalia as regras CEL em memória e publica a decisão em `fraud-decisions.v1` e os alertas (MEDIUM/HIGH) em `fraud-alerts.v1`. Exactly-once, static membership, saúde e métricas em `/actuator/...` pela porta 8084.

Em construção (spec 004). Plano e tarefas: `specs/004-detection-engine/` no repositório `motor-fraude`.

Build: `./mvnw verify`. Camadas: `domain` ← `application` ← `adapters` ← `infrastructure` (ArchUnit).
