package br.com.motorfraude.engine.domain.features;

import java.time.Duration;
import java.time.Instant;

public final class AccountFeatures {

    public static final int MINUTES = 60;
    public static final int HOURS = 24;
    public static final int DAYS = 90;
    public static final int BENEFICIARIES = 50;
    public static final int DEVICES = 10;

    private final BucketRing minutes;
    private final BucketRing hours;
    private final BucketRing days;
    private final RecentSet beneficiaries;
    private final RecentSet devices;

    public AccountFeatures() {
        this(new BucketRing(MINUTES), new BucketRing(HOURS), new BucketRing(DAYS), new RecentSet(BENEFICIARIES), new RecentSet(DEVICES));
    }

    public AccountFeatures(BucketRing minutes, BucketRing hours, BucketRing days, RecentSet beneficiaries, RecentSet devices) {
        this.minutes = minutes;
        this.hours = hours;
        this.days = days;
        this.beneficiaries = beneficiaries;
        this.devices = devices;
    }

    public Baseline baselineFor(Activity activity) {
        long day = unit(activity.occurredAt(), Window.Granularity.DAY);
        long count30d = days.count(day, Window.D30.units);
        long avg = count30d == 0 ? 0 : days.sum(day, Window.D30.units) / count30d;
        long max = days.max(day, DAYS);
        boolean newBeneficiary = !beneficiaries.contains(activity.counterpartyToken());
        boolean newDevice = activity.hasDevice() && !devices.contains(activity.deviceHash());
        return new Baseline(avg, max, newBeneficiary, newDevice);
    }

    public void record(Activity activity) {
        long millis = activity.occurredAt().toEpochMilli();
        minutes.add(unit(activity.occurredAt(), Window.Granularity.MINUTE), activity.amountCents());
        hours.add(unit(activity.occurredAt(), Window.Granularity.HOUR), activity.amountCents());
        days.add(unit(activity.occurredAt(), Window.Granularity.DAY), activity.amountCents());
        beneficiaries.touch(activity.counterpartyToken(), millis);
        if (activity.hasDevice()) {
            devices.touch(activity.deviceHash(), millis);
        }
    }

    public long count(Instant at, Window window) {
        return ring(window).count(unit(at, window.granularity), window.units);
    }

    public long sum(Instant at, Window window) {
        return ring(window).sum(unit(at, window.granularity), window.units);
    }

    public long distinctBeneficiaries(Instant at, Duration window) {
        return beneficiaries.countSince(at.minus(window).toEpochMilli());
    }

    public int deviceCount() {
        return devices.size();
    }

    public BucketRing minutes() {
        return minutes;
    }

    public BucketRing hours() {
        return hours;
    }

    public BucketRing days() {
        return days;
    }

    public RecentSet beneficiaries() {
        return beneficiaries;
    }

    public RecentSet devices() {
        return devices;
    }

    private BucketRing ring(Window window) {
        return switch (window.granularity) {
            case MINUTE -> minutes;
            case HOUR -> hours;
            case DAY -> days;
        };
    }

    private static long unit(Instant at, Window.Granularity granularity) {
        return Math.floorDiv(at.getEpochSecond(), granularity.seconds);
    }
}
