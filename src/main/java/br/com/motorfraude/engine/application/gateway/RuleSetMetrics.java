package br.com.motorfraude.engine.application.gateway;

import java.time.Instant;

public interface RuleSetMetrics {

    RuleSetMetrics NOOP = new RuleSetMetrics() {
        @Override
        public void loaded(long version, Instant publishedAt) {
        }

        @Override
        public void ignored(long version) {
        }

        @Override
        public void failed(String reason) {
        }
    };

    void loaded(long version, Instant publishedAt);

    void ignored(long version);

    void failed(String reason);
}
