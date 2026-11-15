package br.com.motorfraude.engine.application.gateway;

import br.com.motorfraude.engine.domain.Decision;
import java.time.Duration;

public interface EngineMetrics {

    EngineMetrics NOOP = new EngineMetrics() {
        @Override
        public void decided(Decision decision, Duration endToEnd) {
        }

        @Override
        public void duplicateDropped() {
        }

        @Override
        public void lateEvent() {
        }

        @Override
        public void dlq() {
        }
    };

    void decided(Decision decision, Duration endToEnd);

    void duplicateDropped();

    void lateEvent();

    void dlq();
}
