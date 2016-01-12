package org.apache.hyracks.storage.am.statistics.wavelet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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
        this.threshold = threshold;
        this.coefficients = new PriorityQueue<WaveletCoefficient>(threshold, new WaveletCoefficient.ValueComparator());
    }

    @Override
    public long size() {
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
    public void addElement(long index, double value) {
        WaveletCoefficient newCoeff;
        if (coefficients.size() < threshold) {
            newCoeff = new WaveletCoefficient(value, -1, index);
        } else {
            newCoeff = ((Queue<WaveletCoefficient>) coefficients).poll();
            newCoeff.setValue(value);
            newCoeff.setIndex(index);
        }
        ((Collection<WaveletCoefficient>) coefficients).add(newCoeff);
    }

    public void sortOnKey() {
        // sorting coefficients according their indices to enable fast binary search
        List<WaveletCoefficient> sortedCoefficients = new ArrayList<>(coefficients.size());
        Collections.sort(sortedCoefficients, new WaveletCoefficient.KeyComparator());
        coefficients = sortedCoefficients;
    }

    @SuppressWarnings("unchecked")
    @Override
    // Method implements naive synopsis merge, which just picks largest coefficients from the synopsis sum
    public void merge(ISynopsis mergedSynopsis) {
        Collection<? extends Map.Entry<Long, Double>> newCoefficients = new PriorityQueue<WaveletCoefficient>(threshold,
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
        while (mergedIt.hasNext() || it.hasNext()) {
            if ((mergedEntry != null && entry == null) || mergedEntry.getKey() < entry.getKey()) {
                ((Collection<Map.Entry<Long, Double>>) newCoefficients).add(mergedEntry);
                if (mergedIt.hasNext()) {
                    mergedEntry = mergedIt.next();
                } else {
                    mergedEntry = null;
                }
            } else if ((entry != null && mergedEntry == null) || entry.getKey() < mergedEntry.getKey()) {
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

        if (curr.getKey() == coeffIdx) {
            return curr.getValue();
        } else {
            return 0.0;
        }
    }

    public Double pointQuery(Long position, int maxLevel, boolean normalize) {
        Long startCoeffIdx = (1l << (maxLevel - 1)) + (position >> 1);
        PeekingIterator<? extends Entry<Long, Double>> it = new PeekingIterator<>(coefficients.iterator());
        //init with main average
        Double value = findCoeffValue(it, 0l);
        //topmost wavelet coefficient
        Double coeffVal = findCoeffValue(it, 1l)
                * (normalize ? WaveletCoefficient.getNormalizationCoefficient(maxLevel, maxLevel) : 1);
        Long coeffIdx;
        for (int i = maxLevel - 2; i >= -1; i--) {
            if (i < 0) {
                //on the last level use position to calculate sign of the coefficient
                coeffIdx = position;
            } else {
                coeffIdx = startCoeffIdx >> i;
            }
            value += coeffVal * ((coeffIdx & 0x1) == 0 ? 1 : -1);
            coeffVal = findCoeffValue(it, coeffIdx)
                    * (normalize ? WaveletCoefficient.getNormalizationCoefficient(maxLevel, i) : 1);
        }
        return value;
    }

    //    public Double rangeQuery(Long startPosition, Long endPosition, int maxLevel, boolean normalize) {
    //        Double value = 0.0;
    //        PeekingIterator<? extends Entry<Long, Double>> it = new PeekingIterator<>(coefficients.iterator());
    //        Double mainAvg = findCoeffValue(it, 0l);
    //        List<DyadicTupleRange<Double>> workingSet = Lists.newArrayList(new DyadicTupleRange<Double>(
    //                startPosition + (1l << maxLevel), endPosition + (1l << maxLevel), mainAvg));
    //        int level = maxLevel - 1;
    //        while (!workingSet.isEmpty()) {
    //            ListIterator<DyadicTupleRange<Double>> rangeIt = workingSet.listIterator();
    //
    //            while (rangeIt.hasNext()) {
    //                DyadicTupleRange<Double> range = rangeIt.next();
    //                Double coeff = findCoeffValue(it, range.getStart() >> (level + 1))
    //                        * (normalize ? WaveletCoefficient.getNormalizationCoefficient(maxLevel, level + 1) : 1);
    //                if (range.getEnd() - range.getStart() + 1 == (1l << (level + 1))) {
    //                    //range is a full dyadic subrange
    //                    rangeIt.remove();
    //                    Double rangeValue = 0.0;
    //                    if (level == -1) {
    //                        rangeValue = (((range.getStart() - (1l << maxLevel)) & 0x1) == 0) ? range.getValue() + coeff
    //                                : range.getValue() - coeff;
    //                    } else {
    //                        rangeValue = range.getValue() * (1l << (level + 1));
    //                    }
    //                    value += rangeValue;
    //                } else {
    //                    long startCoeff = range.getStart() >> level;
    //                    long endCoeff = range.getEnd() >> level;
    //                    //split the range
    //                    if (startCoeff != endCoeff) {
    //                        rangeIt.remove();
    //                        rangeIt.add(new DyadicTupleRange<Double>(range.getStart(), (endCoeff << level) - 1,
    //                                range.getValue() + coeff));
    //                        rangeIt.add(new DyadicTupleRange<Double>(endCoeff << level, range.getEnd(),
    //                                range.getValue() - coeff));
    //                        //                        rangeIt.previous();
    //                        //                        rangeIt.previous();
    //                    } else {
    //                        rangeIt.remove();
    //                        rangeIt.add(new DyadicTupleRange<Double>(range.getStart(), range.getEnd(),
    //                                range.getValue() + coeff * (((startCoeff & 0x1) == 0) ? 1 : -1)));
    //                    }
    //                }
    //            }
    //            //            workingSet.addAll(newWorkingSet);
    //            //            newWorkingSet.clear();
    //            level--;
    //        }
    //        return value;
    //    }

}