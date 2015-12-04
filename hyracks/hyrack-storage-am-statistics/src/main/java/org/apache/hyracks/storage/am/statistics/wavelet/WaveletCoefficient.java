package org.apache.hyracks.storage.am.statistics.wavelet;

import java.util.Map.Entry;
import java.util.Objects;

import org.apache.hyracks.data.std.api.IComparable;
import org.apache.hyracks.data.std.api.INumeric;
import org.apache.hyracks.data.std.api.IPointable;

public class WaveletCoefficient<K extends IPointable /*IMath<K>*/, V extends IPointable & IComparable /* extends IMath<V>*/>
        implements Entry<K, V>, Comparable<WaveletCoefficient<K, V>> {

    public V value;
    public int level;
    public K index;

    public WaveletCoefficient() {
    }

    public WaveletCoefficient(V value, int level, K index) {
        this.value = value;
        this.level = level;
        this.index = index;
    }

    @Override
    public K getKey() {
        return index;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        this.value = value;
        return this.value;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public void setIndex(K index) {
        this.index = index;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WaveletCoefficient))
            return false;
        @SuppressWarnings("unchecked")
        WaveletCoefficient<K, V> triple = (WaveletCoefficient<K, V>) o;
        return triple.value.equals(value) && triple.level == level && triple.index.equals(index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, level, index);
    }

    public static Double getNormalizationCoefficient(int maxLevel, int level) {
        return (1 << ((maxLevel - level) / 2)) * ((((maxLevel - level) % 2) == 0) ? 1 : Math.sqrt(2));
    }

    public static <K extends INumeric> int getLevel(K coeffPointable, int maxLevel) {
        long coeffIdx = coeffPointable.longValue();
        if (coeffIdx == 0)
            return maxLevel;
        int level = -1;
        while (coeffIdx > 0) {
            coeffIdx = coeffIdx >> 1;
            level++;
        }
        return maxLevel - level;
    }

    @Override
    // default comparator based on coefficient value
    public int compareTo(WaveletCoefficient<K, V> o) {
        return value.compareTo(o.getValue());
    }
}