package br.com.motorfraude.engine.fakes;

import br.com.motorfraude.engine.application.gateway.EngineMetrics;
import br.com.motorfraude.engine.domain.Decision;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public final class RecordingEngineMetrics implements EngineMetrics {

    public final List<Decision> decisions = new ArrayList<>();
    public final List<Duration> latencies = new ArrayList<>();
    public int duplicates;
    public int late;
    public int dlq;

    @Override
    public void decided(Decision decision, Duration endToEnd) {
        decisions.add(decision);
        latencies.add(endToEnd);
    }

    @Override
    public void duplicateDropped() {
        duplicates++;
    }

    @Override
    public void lateEvent() {
        late++;
    }

    @Override
    public void dlq() {
        dlq++;
    }
}
