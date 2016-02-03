package org.apache.hyracks.storage.am.statistics.wavelet;

public class DyadicTupleRange {
    private long start;
    private long end;
    private double value;

    public DyadicTupleRange(long start, long end, double value) {
        this.start = start;
        this.end = end;
        this.value = value;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public double getValue() {
        return value;
    }
}
