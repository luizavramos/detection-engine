package br.com.motorfraude.engine.domain;

import br.com.motorfraude.contracts.ContextCompleteness;
import br.com.motorfraude.contracts.EvaluateTransaction;
import br.com.motorfraude.contracts.FraudAlert;
import br.com.motorfraude.contracts.FraudDecision;
import br.com.motorfraude.contracts.RuleHit;
import br.com.motorfraude.contracts.Severity;
import br.com.motorfraude.engine.domain.features.FeatureSnapshot;
import br.com.motorfraude.rules.kernel.evaluation.Evaluation;
import br.com.motorfraude.rules.kernel.evaluation.RuleError;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

public final class Decision {

    private static final Pattern TRACE_ID = Pattern.compile("[0-9a-f]{32}");

    private final FraudDecision decisionMessage;
    private final FraudAlert alertMessage;
    private final List<RuleError> ruleErrors;

    private Decision(FraudDecision decisionMessage, FraudAlert alertMessage, List<RuleError> ruleErrors) {
        this.decisionMessage = decisionMessage;
        this.alertMessage = alertMessage;
        this.ruleErrors = ruleErrors;
    }

    public static Decision from(EvaluateTransaction tx, Evaluation evaluation, FeatureSnapshot snapshot, ContextCompleteness completeness,
            Instant receivedAt, Instant decidedAt, String traceId) {
        var severity = Severity.valueOf(evaluation.severity().name());
        var hits = evaluation.activeHits().stream().map(Decision::hit).toList();
        var shadow = evaluation.shadowHits().stream().map(Decision::hit).toList();
        boolean alert = severity != Severity.NONE;
        var transactionId = tx.transactionId();

        var message = new FraudDecision(
                Uuids.v5(Uuids.NAMESPACE, "decision:" + transactionId).toString(),
                transactionId,
                tx.accountId(),
                alert ? br.com.motorfraude.contracts.Decision.ALERT : br.com.motorfraude.contracts.Decision.NO_ALERT,
                evaluation.score(),
                severity,
                hits,
                shadow,
                snapshot.values(),
                evaluation.ruleSetVersion(),
                completeness,
                receivedAt,
                decidedAt);

        FraudAlert alertMessage = alert
                ? new FraudAlert(Uuids.v5(Uuids.NAMESPACE, transactionId).toString(), transactionId, tx.accountId(), severity,
                        evaluation.score(), hits, evaluation.ruleSetVersion(), completeness, receivedAt, decidedAt,
                        traceIdOrDerived(traceId, transactionId))
                : null;
        return new Decision(message, alertMessage, evaluation.errors());
    }

    public Severity severity() {
        return decisionMessage.severity();
    }

    public boolean isAlert() {
        return alertMessage != null;
    }

    public FraudDecision decisionMessage() {
        return decisionMessage;
    }

    public Optional<FraudAlert> alertMessage() {
        return Optional.ofNullable(alertMessage);
    }

    public List<RuleError> ruleErrors() {
        return ruleErrors;
    }

    private static RuleHit hit(br.com.motorfraude.rules.kernel.evaluation.RuleHit hit) {
        return new RuleHit(hit.ruleId(), hit.version(), hit.weight(), hit.reason());
    }

    private static String traceIdOrDerived(String traceId, String transactionId) {
        if (traceId != null && TRACE_ID.matcher(traceId).matches() && !traceId.equals("00000000000000000000000000000000")) {
            return traceId;
        }
        return Uuids.v5(Uuids.NAMESPACE, "trace:" + transactionId).toString().replace("-", "");
    }
}
