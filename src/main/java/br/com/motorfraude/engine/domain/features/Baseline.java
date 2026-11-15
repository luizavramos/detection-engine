package br.com.motorfraude.engine.domain.features;

public record Baseline(long avgAmount30d, long maxAmount90d, boolean newBeneficiary, boolean newDevice) {
}
