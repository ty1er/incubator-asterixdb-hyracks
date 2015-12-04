package org.apache.hyracks.storage.am.statistics.wavelet;

import java.util.Objects;

public class WaveletTuple implements Comparable<WaveletTuple> {
    private long position;
    private double frequency;

    public WaveletTuple(long position, double frequency) {
        this.position = position;
        this.frequency = frequency;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public double getFrequency() {
        return frequency;
    }

    public void setFrequency(double frequency) {
        this.frequency = frequency;
    }

    @Override
    public int compareTo(WaveletTuple o) {
        return Long.compare(position, o.position);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WaveletTuple))
            return false;
        WaveletTuple tuple = (WaveletTuple) o;
        return tuple.position == position;
    }

    @Override
    public int hashCode() {
        return Objects.hash(position);
    }
}