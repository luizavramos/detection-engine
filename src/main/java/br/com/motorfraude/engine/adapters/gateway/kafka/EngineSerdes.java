package br.com.motorfraude.engine.adapters.gateway.kafka;

import br.com.motorfraude.contracts.BlocklistEntry;
import br.com.motorfraude.contracts.CustomerProfile;
import br.com.motorfraude.contracts.EvaluateTransaction;
import br.com.motorfraude.contracts.FraudAlert;
import br.com.motorfraude.contracts.FraudDecision;
import br.com.motorfraude.contracts.TransactionHistoryRecord;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;

public final class EngineSerdes {

    private EngineSerdes() {
    }

    public static Serde<EvaluateTransaction> command() {
        return JsonSerde.validated(EvaluateTransaction.class, tx -> {
            require(tx, "mensagem vazia");
            require(tx.transactionId(), "transactionId");
            require(tx.accountId(), "accountId");
            require(tx.type(), "type");
            require(tx.amount(), "amount");
            require(tx.counterparty(), "counterparty");
            require(tx.counterparty().accountToken(), "counterparty.accountToken");
            require(tx.channel(), "channel");
            require(tx.occurredAt(), "occurredAt");
        });
    }

    public static Serde<TransactionHistoryRecord> history() {
        return JsonSerde.validated(TransactionHistoryRecord.class, record -> {
            require(record, "mensagem vazia");
            require(record.transactionId(), "transactionId");
            require(record.accountId(), "accountId");
            require(record.amount(), "amount");
            require(record.counterpartyToken(), "counterpartyToken");
            require(record.occurredAt(), "occurredAt");
        });
    }

    public static Serde<CustomerProfile> profile() {
        return JsonSerde.validated(CustomerProfile.class, profile -> {
            require(profile, "mensagem vazia");
            require(profile.accountId(), "accountId");
            require(profile.segment(), "segment");
            require(profile.accountOpenedAt(), "accountOpenedAt");
        });
    }

    public static Serde<BlocklistEntry> blocklist() {
        return JsonSerde.validated(BlocklistEntry.class, entry -> {
            require(entry, "mensagem vazia");
            require(entry.entityToken(), "entityToken");
            require(entry.entityType(), "entityType");
        });
    }

    public static Serde<FraudDecision> decision() {
        return JsonSerde.of(FraudDecision.class);
    }

    public static Serde<FraudAlert> alert() {
        return JsonSerde.of(FraudAlert.class);
    }

    private static void require(Object value, String field) {
        if (Objects.isNull(value)) {
            throw new IllegalArgumentException("campo obrigatório ausente: " + field);
        }
    }
}
