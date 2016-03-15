/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.storage.am.statistics.wavelet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ISynopsis;
import org.apache.hyracks.storage.am.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsis;

public class WaveletSynopsis extends AbstractSynopsis<WaveletCoefficient> {

    private static final long serialVersionUID = 1L;

    private Collection<WaveletCoefficient> coefficients;

    public WaveletSynopsis(ITypeTraits keyTypeTraits, Collection<WaveletCoefficient> coefficients, int size)
            throws HyracksDataException {
        super(keyTypeTraits, size);
        this.coefficients = coefficients;
    }

    public WaveletSynopsis(ITypeTraits keyTypeTraits, int size) throws HyracksDataException {
        this(keyTypeTraits, new PriorityQueue<WaveletCoefficient>(size, new WaveletCoefficient.ValueComparator()),
                size);
    }

    @Override
    public SynopsisType getType() {
        return SynopsisType.Wavelet;
    }

    @Override
    public Iterator<WaveletCoefficient> iterator() {
        return coefficients.iterator();
    }

    // Adds a new coefficient to the transform, subject to thresholding
    public void addElement(long index, double value, int maxLevel) {
        WaveletCoefficient newCoeff;
        if (coefficients.size() < size) {
            newCoeff = new WaveletCoefficient(value / WaveletCoefficient.getNormalizationCoefficient(maxLevel,
                    WaveletCoefficient.getLevel(index, maxLevel)), -1, index);
        } else {
            newCoeff = ((Queue<WaveletCoefficient>) coefficients).poll();
            newCoeff.setValue(value);
            newCoeff.setIndex(index);
        }
        coefficients.add(newCoeff);
    }

    public void sortOnKey() {
        // sorting coefficients according their indices to enable fast binary search
        List<WaveletCoefficient> sortedCoefficients = new ArrayList<>(coefficients);
        Collections.sort(sortedCoefficients, new WaveletCoefficient.KeyComparator());
        coefficients = sortedCoefficients;
    }

    @Override
    // Method implements naive synopsis merge, which just picks largest coefficients from the synopsis sum
    public void merge(ISynopsis<WaveletCoefficient> mergedSynopsis) {
        if (mergedSynopsis.getType() != SynopsisType.Wavelet) {
            return;
        }
        Collection<WaveletCoefficient> newCoefficients = new PriorityQueue<WaveletCoefficient>(
                new WaveletCoefficient.ValueComparator());
        //method assumes that the synopsis coefficients are already sorted on key
        Iterator<WaveletCoefficient> mergedIt = mergedSynopsis.iterator();
        Iterator<WaveletCoefficient> it = iterator();
        WaveletCoefficient mergedEntry = null;
        WaveletCoefficient entry = null;
        if (mergedIt.hasNext()) {
            mergedEntry = mergedIt.next();
        }
        if (it.hasNext()) {
            entry = it.next();
        }
        while (mergedEntry != null || entry != null) {
            if ((mergedEntry != null && entry == null)
                    || (mergedEntry != null && entry != null && mergedEntry.getKey() < entry.getKey())) {
                newCoefficients.add(mergedEntry);
                if (mergedIt.hasNext()) {
                    mergedEntry = mergedIt.next();
                } else {
                    mergedEntry = null;
                }
            } else if ((entry != null && mergedEntry == null)
                    || (mergedEntry != null && entry != null && entry.getKey() < mergedEntry.getKey())) {
                newCoefficients.add(entry);
                if (it.hasNext()) {
                    entry = it.next();
                } else {
                    entry = null;
                }
            } else {
                newCoefficients
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

    private Double findCoeffValue(PeekingIterator<? extends ISynopsisElement> coeffIt, Long coeffIdx) {
        ISynopsisElement curr;
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
    public Double pointQuery(Long position) {
        Long startCoeffIdx = (1l << (maxLevel - 1)) + ((position - domainStart) >> 1);
        PeekingIterator<? extends ISynopsisElement> it = new PeekingIterator<>(coefficients.iterator());
        //init with main average
        Double value = findCoeffValue(it, 0l);
        //topmost wavelet coefficient
        Double coeffVal = findCoeffValue(it, 1l) * WaveletCoefficient.getNormalizationCoefficient(maxLevel, maxLevel);
        Long coeffIdx;
        for (int i = maxLevel - 1; i >= 0; i--) {
            if (i == 0) {
                //on the last level use position to calculate sign of the coefficient
                coeffIdx = position - domainStart;
            } else {
                coeffIdx = startCoeffIdx >> (i - 1);
            }
            value += coeffVal * ((coeffIdx & 0x1) == 0 ? 1 : -1);
            coeffVal = findCoeffValue(it, coeffIdx) * WaveletCoefficient.getNormalizationCoefficient(maxLevel, i);
        }
        return value;
    }

    @Override
    public Double rangeQuery(Long startPosition, Long endPosition) {
        Double value = 0.0;
        PeekingIterator<? extends ISynopsisElement> it = new PeekingIterator<>(coefficients.iterator());
        Double mainAvg = findCoeffValue(it, 0l);
        List<DyadicTupleRange> workingSet = new ArrayList<>();
        workingSet.add(new DyadicTupleRange(startPosition - domainStart, endPosition - domainStart, mainAvg));
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