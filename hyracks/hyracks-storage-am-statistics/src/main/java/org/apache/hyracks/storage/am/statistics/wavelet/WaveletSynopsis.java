package org.apache.hyracks.storage.am.statistics.wavelet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.hyracks.storage.am.common.api.ISynopsis;

public class WaveletSynopsis implements ISynopsis, Serializable {

    private static final long serialVersionUID = 1L;

    private Collection<? extends Map.Entry<Long, Double>> coefficients;
    private final int threshold;

    public WaveletSynopsis(Collection<WaveletCoefficient> coefficients) {
        this.coefficients = coefficients;
        this.threshold = coefficients.size();
    }

    public WaveletSynopsis(int threshold) {
        this.coefficients = new PriorityQueue<WaveletCoefficient>(threshold, new WaveletCoefficient.ValueComparator());
        this.threshold = threshold;
    }

    @Override
    public int size() {
        return threshold;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Entry<Long, Double>> iterator() {
        Iterator<? extends Entry<Long, Double>> it = coefficients.iterator();
        return ((Iterator<Entry<Long, Double>>) it);
    }

    @SuppressWarnings("unchecked")
    @Override
    // Adds a new coefficient to the transform, subject to thresholding
    public void addElement(long index, double value, int maxLevel) {
        WaveletCoefficient newCoeff;
        if (coefficients.size() < threshold) {
            newCoeff = new WaveletCoefficient(value / WaveletCoefficient.getNormalizationCoefficient(maxLevel,
                    WaveletCoefficient.getLevel(index, maxLevel)), -1, index);
        } else {
            newCoeff = ((Queue<WaveletCoefficient>) coefficients).poll();
            newCoeff.setValue(value);
            newCoeff.setIndex(index);
        }
        ((Collection<WaveletCoefficient>) coefficients).add(newCoeff);
    }

    public void sortOnKey() {
        // sorting coefficients according their indices to enable fast binary search
        List<WaveletCoefficient> sortedCoefficients = new ArrayList<>((Collection<WaveletCoefficient>) coefficients);
        Collections.sort(sortedCoefficients, new WaveletCoefficient.KeyComparator());
        coefficients = sortedCoefficients;
    }

    @SuppressWarnings("unchecked")
    @Override
    // Method implements naive synopsis merge, which just picks largest coefficients from the synopsis sum
    public void merge(ISynopsis mergedSynopsis) {
        Collection<? extends Map.Entry<Long, Double>> newCoefficients = new PriorityQueue<WaveletCoefficient>(
                new WaveletCoefficient.ValueComparator());
        //method assumes that the synopsis coefficients are already sorted on key
        Iterator<Map.Entry<Long, Double>> mergedIt = mergedSynopsis.iterator();
        Iterator<Map.Entry<Long, Double>> it = iterator();
        Map.Entry<Long, Double> mergedEntry = null;
        Map.Entry<Long, Double> entry = null;
        if (mergedIt.hasNext()) {
            mergedEntry = mergedIt.next();
        }
        if (it.hasNext()) {
            entry = it.next();
        }
        while (mergedEntry != null || entry != null) {
            if ((mergedEntry != null && entry == null)
                    || (mergedEntry != null && entry != null && mergedEntry.getKey() < entry.getKey())) {
                ((Collection<Map.Entry<Long, Double>>) newCoefficients).add(mergedEntry);
                if (mergedIt.hasNext()) {
                    mergedEntry = mergedIt.next();
                } else {
                    mergedEntry = null;
                }
            } else if ((entry != null && mergedEntry == null)
                    || (mergedEntry != null && entry != null && entry.getKey() < mergedEntry.getKey())) {
                ((Collection<Map.Entry<Long, Double>>) newCoefficients).add(entry);
                if (it.hasNext()) {
                    entry = it.next();
                } else {
                    entry = null;
                }
            } else {
                ((Collection<WaveletCoefficient>) newCoefficients)
                        .add(new WaveletCoefficient(entry.getValue() + mergedEntry.getValue(), -1, entry.getKey()));
                if (mergedIt.hasNext()) {
                    mergedEntry = mergedIt.next();
                } else {
                    mergedEntry = null;
                }
                if (it.hasNext()) {
                    entry = it.next();
                } else {
                    entry = null;
                }
            }
        }
        coefficients = newCoefficients;
        sortOnKey();
    }

    @Override
    public void merge(List<ISynopsis> synopsisList) {
        for (ISynopsis s : synopsisList) {
            merge(s);
        }
    }

    private Double findCoeffValue(PeekingIterator<? extends Entry<Long, Double>> coeffIt, Long coeffIdx) {
        Entry<Long, Double> curr;
        try {
            curr = coeffIt.element();
            while (curr.getKey() < coeffIdx) {
                curr = coeffIt.next();
            }
        } catch (NoSuchElementException e) {
            return 0.0;
        }

        if (curr.getKey().equals(coeffIdx)) {
            return curr.getValue();
        } else {
            return 0.0;
        }
    }

    @Override
    public Double pointQuery(Long position, int maxLevel) {
        Long startCoeffIdx = (1l << (maxLevel - 1)) + (position >> 1);
        PeekingIterator<? extends Entry<Long, Double>> it = new PeekingIterator<>(coefficients.iterator());
        //init with main average
        Double value = findCoeffValue(it, 0l);
        //topmost wavelet coefficient
        Double coeffVal = findCoeffValue(it, 1l) * WaveletCoefficient.getNormalizationCoefficient(maxLevel, maxLevel);
        Long coeffIdx;
        for (int i = maxLevel - 1; i >= 0; i--) {
            if (i == 0) {
                //on the last level use position to calculate sign of the coefficient
                coeffIdx = position;
            } else {
                coeffIdx = startCoeffIdx >> (i - 1);
            }
            value += coeffVal * ((coeffIdx & 0x1) == 0 ? 1 : -1);
            coeffVal = findCoeffValue(it, coeffIdx) * WaveletCoefficient.getNormalizationCoefficient(maxLevel, i);
        }
        return value;
    }

    @Override
    public Double rangeQuery(Long startPosition, Long endPosition, int maxLevel) {
        Double value = 0.0;
        PeekingIterator<? extends Entry<Long, Double>> it = new PeekingIterator<>(coefficients.iterator());
        Double mainAvg = findCoeffValue(it, 0l);
        List<DyadicTupleRange> workingSet = new ArrayList<>();
        workingSet.add(new DyadicTupleRange(startPosition, endPosition, mainAvg));
        int level = maxLevel;
        while (!workingSet.isEmpty()) {
            ListIterator<DyadicTupleRange> rangeIt = workingSet.listIterator();

            while (rangeIt.hasNext()) {
                DyadicTupleRange range = rangeIt.next();
                long startIdx = (range.getStart() >> level) + (1l << (maxLevel - level));
                Double coeff = findCoeffValue(it, startIdx)
                        * WaveletCoefficient.getNormalizationCoefficient(maxLevel, level);
                if (range.getEnd() - range.getStart() + 1 == (1l << level)) {
                    //range is a full dyadic subrange
                    rangeIt.remove();
                    Double rangeValue = 0.0;
                    if (level == 0) {
                        rangeValue = ((startIdx & 0x1) == 0) ? range.getValue() + coeff : range.getValue() - coeff;
                    } else {
                        rangeValue = range.getValue() * (1l << level);
                    }
                    value += rangeValue;
                } else {
                    long startPos = (range.getStart() >> (level - 1)) + (1l << (maxLevel - level + 1));
                    long endPos = (range.getEnd() >> (level - 1)) + (1l << (maxLevel - level + 1));
                    long splitPos = (endPos - (1l << (maxLevel - level + 1))) << (level - 1);
                    //split the range
                    if (startPos != endPos) {
                        rangeIt.remove();
                        rangeIt.add(new DyadicTupleRange(range.getStart(), splitPos - 1, range.getValue() + coeff));
                        rangeIt.add(new DyadicTupleRange(splitPos, range.getEnd(), range.getValue() - coeff));
                    } else {
                        rangeIt.remove();
                        rangeIt.add(new DyadicTupleRange(range.getStart(), range.getEnd(),
                                range.getValue() + coeff * (((startPos & 0x1) == 0) ? 1 : -1)));
                    }
                }
            }
            level--;
        }
        return value;
    }

}