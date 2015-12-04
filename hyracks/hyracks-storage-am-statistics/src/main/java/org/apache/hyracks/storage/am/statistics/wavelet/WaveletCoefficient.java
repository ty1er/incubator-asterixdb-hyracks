package org.apache.hyracks.storage.am.statistics.wavelet;

import java.util.Objects;

import org.apache.hyracks.data.std.primitive.DoublePointable;

public class WaveletCoefficient /*implements Comparable<WaveletCoefficient>*/ {

    private double value;
    private int level;
    private long index;

    public WaveletCoefficient(double value, int level, long index) {
        this.value = value;
        this.level = level;
        this.index = index;
    }

    public long getIndex() {
        return index;
    }

    public double getValue() {
        return value;
    }

    public int getLevel() {
        return level;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WaveletCoefficient))
            return false;
        WaveletCoefficient coeff = (WaveletCoefficient) o;
        return (coeff.value - value) < DoublePointable.getEpsilon() && (coeff.level == level) && (coeff.index == index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, level, index);
    }

    public static double getNormalizationCoefficient(int maxLevel, int level) {
        return (1 << ((maxLevel - level) / 2)) * ((((maxLevel - level) % 2) == 0) ? 1 : Math.sqrt(2));
    }

    // Returns index of the parent coefficient
    public long getParentCoeffIndex(long domainMin, long maxLevel) {
        // Special case for values on level 0
        if (level == 0)
            // Convert position to proper coefficient index
            return (index >> 1) - (domainMin >> 1) + (1l << (maxLevel - 1));
        else
            return index >>> 1;
    }

    // Returns true if the coefficient's dyadic range covers tuple with the given position
    public boolean covers(long tuplePosition, long maxLevel, long domainMin) {
        if (level < 0)
            return true;
        else if (level == 0)
            return index == tuplePosition;
        else
            return index == (((tuplePosition - domainMin) >>> 1) + (1l << (maxLevel - 1))) >>> (level - 1);
    }

    //    public static <K extends INumeric> int getLevel(K coeffPointable, int maxLevel) {
    //        long coeffIdx = coeffPointable.longValue();
    //        if (coeffIdx == 0)
    //            return maxLevel;
    //        int level = -1;
    //        while (coeffIdx > 0) {
    //            coeffIdx = coeffIdx >> 1;
    //            level++;
    //        }
    //        return maxLevel - level;
    //    }
}