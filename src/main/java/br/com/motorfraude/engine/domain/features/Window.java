package br.com.motorfraude.engine.domain.features;

public enum Window {
    M1(Granularity.MINUTE, 1),
    M5(Granularity.MINUTE, 5),
    H1(Granularity.MINUTE, 60),
    H24(Granularity.HOUR, 24),
    D30(Granularity.DAY, 30);

    public enum Granularity {
        MINUTE(60),
        HOUR(3_600),
        DAY(86_400);

        final long seconds;

        Granularity(long seconds) {
            this.seconds = seconds;
        }
    }

    final Granularity granularity;
    final int units;

    Window(Granularity granularity, int units) {
        this.granularity = granularity;
        this.units = units;
    }
}
