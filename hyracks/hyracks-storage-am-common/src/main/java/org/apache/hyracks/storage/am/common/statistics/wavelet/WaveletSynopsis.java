package org.apache.hyracks.storage.am.common.statistics.wavelet;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Stack;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.IPrimitiveIntegerValueProvider;
import org.apache.hyracks.storage.am.common.api.IPrimitiveIntegerValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.statistics.Synopsis;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class WaveletSynopsis/*<K extends IMath<K>, V extends IMath<V>>*/ extends Synopsis<Integer, Double> {

    private final static int METADATA_PAGE_ID = 0;
    private final static int NUM_PAGES_OFFSET = 0;
    private final static int NUM_ELEMENTS_OFFSET = NUM_PAGES_OFFSET + 4;

    private final int[] waveletFields;
    private final IPrimitiveIntegerValueProvider waveletFieldValueProvider;
    private final PriorityQueue<WaveletCoefficient<Integer, Double>> coefficients;
    private final long threshold;

    private final int numPages;

    public WaveletSynopsis(IBufferCache bufferCache, IFileMapProvider fileMapProvider, FileReference file,
            int[] keyFields, int threshold, IPrimitiveIntegerValueProviderFactory valueProviderFactory) {
        super(bufferCache, fileMapProvider, file);
        this.waveletFields = keyFields;
        this.waveletFieldValueProvider = valueProviderFactory.createPrimitiveIntegerValueProvider();
        this.threshold = threshold;
        this.coefficients = new PriorityQueue<>(threshold);
        this.numPages = (int) Math.ceil(threshold * (4 + 8) / (double) bufferCache.getPageSize());
    }

    @Override
    public int getNumPages() throws HyracksDataException {
        if (!isActive) {
            throw new HyracksDataException("The bloom filter is not activated.");
        }
        return numPages;
    }

    private void initWaveletSynopsisMetaData() throws HyracksDataException {
        ICachedPage metaPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, METADATA_PAGE_ID), true);
        metaPage.acquireWriteLatch();
        try {
            metaPage.getBuffer().putInt(NUM_PAGES_OFFSET, 0);
            metaPage.getBuffer().putLong(NUM_ELEMENTS_OFFSET, 0L);
        } finally {
            metaPage.releaseWriteLatch(true);
            bufferCache.unpin(metaPage);
        }
    }

    @Override
    // Adds a new coefficient to the transform, subject to thresholding
    public void addElement(Integer index, Double value) {
        WaveletCoefficient<Integer, Double> newCoeff;
        if (coefficients.size() < threshold)
            newCoeff = new WaveletCoefficient<Integer, Double>(value, 0, index);
        else {
            newCoeff = coefficients.poll();
            newCoeff.setValue(value);
            newCoeff.setIndex(index);
        }
        coefficients.add(newCoeff);

    }

    @Override
    public IIndexBulkLoader createBuilder() throws HyracksDataException {
        return new SparseTransformBuilder();
    }

    public class SparseTransformBuilder implements IIndexBulkLoader {
        private final Stack<WaveletCoefficient<Integer, Double>> avgStack;
        private Pair<Integer, Double> curr;
        private long transformPos;
        private final long domainEnd;
        private int lastLevel;
        private final List<Pair<Integer, Double>> borderTuples;

        public SparseTransformBuilder() throws HyracksDataException {
            avgStack = new Stack<>();
            borderTuples = new ArrayList<>(2);
            // initial transform element
            transformPos = Byte.MIN_VALUE;//(int) waveletFieldPointer.minDomainValue();
            domainEnd = Byte.MAX_VALUE;
            lastLevel = 0;

            persistWaveletSynopsisMetaData();
        }

        private void persistWaveletSynopsisMetaData() throws HyracksDataException {
            ICachedPage metaPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, METADATA_PAGE_ID), false);
            metaPage.acquireWriteLatch();
            try {
                metaPage.getBuffer().putInt(NUM_PAGES_OFFSET, numPages);
                metaPage.getBuffer().putLong(NUM_ELEMENTS_OFFSET, threshold);
            } finally {
                metaPage.releaseWriteLatch(true);
                bufferCache.unpin(metaPage);
            }
        }

        @Override
        public void add(ITupleReference tuple) throws IndexException, HyracksDataException {
            //            if (waveletFields.length > 1)
            //                throw new HyracksDataException("Wavelet synopsis does not support composite keys");
            curr = Pair.of((int) waveletFieldValueProvider.getValue(tuple.getFieldData(waveletFields[0]),
                    tuple.getFieldStart(waveletFields[0])), 1.0);
            final int maxLevel = waveletFieldValueProvider.maxLevel(tuple.getFieldData(waveletFields[0]),
                    tuple.getFieldStart(waveletFields[0]));

            while (transformPos <= curr.getKey()) {
                // current position is a left border of dyadic range
                if (curr.getKey() == transformPos) {
                    borderTuples.add(Pair.of(0, (double) curr.getValue()));
                    return;
                }
                int newLevel = (int) Math.floor(Math.log((double) (curr.getKey()) - transformPos + 1) / Math.log(2));
                long levelRightBorder = 1l << newLevel;
                //add first dummy average
                if (avgStack.isEmpty()) {
                    avgStack.push(new WaveletCoefficient<Integer, Double>(0.0, maxLevel, 0));
                    lastLevel = newLevel;
                }

                // current position is a right border of dyadic range
                if (curr.getKey() == transformPos + levelRightBorder - 1 /*&& curr.position != domainMax*/)
                    borderTuples.add(Pair.of((int) (levelRightBorder - 1), (double) curr.getValue()));

                WaveletCoefficient<Integer, Double> newCoeff;
                WaveletCoefficient<Integer, Double> topCoeff = avgStack.peek();
                if (newLevel >= lastLevel) {
                    topCoeff = computeDyadicRange(lastLevel, maxLevel, topCoeff, borderTuples);
                    newCoeff = topCoeff;
                    do {
                        WaveletCoefficient<Integer, Double> oldCoeff = avgStack.pop();
                        //skip first dummy coefficient
                        if (oldCoeff.index > 0)
                            newCoeff = average(oldCoeff, newCoeff, waveletFieldValueProvider.maxLevel(
                                    tuple.getFieldData(waveletFields[0]), tuple.getFieldStart(waveletFields[0])));
                    } while (!avgStack.isEmpty() && avgStack.peek().level == newCoeff.level);
                } else {
                    newCoeff = computeDyadicRange(newLevel, maxLevel, topCoeff, borderTuples);
                    topCoeff = newCoeff;
                }
                avgStack.push(newCoeff);
                transformPos += 1l << topCoeff.level;
                lastLevel = newCoeff.level;

                borderTuples.clear();
            }
        }

        private WaveletCoefficient<Integer, Double> average(WaveletCoefficient<Integer, Double> oldCoeff,
                WaveletCoefficient<Integer, Double> newCoeff, int maxLevel) {
            Integer coeffIdx = oldCoeff.index >> 1;
            addElement(coeffIdx, (oldCoeff.value - newCoeff.value)
                    / (2.0 * WaveletCoefficient.getNormalizationCoefficient(maxLevel, oldCoeff.level + 1)));
            WaveletCoefficient<Integer, Double> topCoeff = new WaveletCoefficient<Integer, Double>(
                    (oldCoeff.value + newCoeff.value) / 2.0, oldCoeff.level + 1, coeffIdx);
            return topCoeff;
        }

        private WaveletCoefficient<Integer, Double> computeDyadicRange(int level, int maxLevel,
                WaveletCoefficient<Integer, Double> topCoeff, List<Pair<Integer, Double>> borderTuples) {
            //short circuit coefficient computation for 0
            if (borderTuples.isEmpty()) {
                Integer coeffIdx = ((topCoeff.index + 1) << (topCoeff.level - level));
                return new WaveletCoefficient<Integer, Double>(0.0, level, coeffIdx);
            }

            Map<Integer, Double> newCoefs = new HashMap<>();
            Double avg = 0.0;
            Integer coeffIdx = -1;
            for (int i = 1; i <= level; i++) {
                newCoefs.clear();
                avg = 0.0;
                for (int j = 0; j < borderTuples.size(); j++) {
                    Pair<Integer, Double> item = borderTuples.get(j);
                    coeffIdx = ((topCoeff.index + 1) << (topCoeff.level - i)) + (item.getKey() >> 1);
                    Double newValue = item.getValue() / 2.0;
                    Double oldValue = newCoefs.containsKey(coeffIdx) ? newCoefs.get(coeffIdx) : 0;
                    if ((item.getKey() & 0x1) == 1) {
                        newCoefs.put(coeffIdx, (oldValue - newValue));
                    } else {
                        newCoefs.put(coeffIdx, (oldValue + newValue));
                    }
                    avg += newValue;
                    borderTuples.set(j, Pair.of(item.getKey() >> 1, newValue));
                }

                for (Entry<Integer, Double> e : newCoefs.entrySet())
                    addElement(e.getKey(), e.getValue() / WaveletCoefficient.getNormalizationCoefficient(maxLevel, i));
            }
            return new WaveletCoefficient<Integer, Double>(avg, level, coeffIdx);
        }

        @Override
        public void end() throws IndexException, HyracksDataException {
            //assert(avgStack.size() == 1);
            if (curr.getKey() != domainEnd) {

            }
            WaveletCoefficient<Integer, Double> topCoeff = avgStack.pop();
            topCoeff.index = 0;
            addElement(topCoeff.index, topCoeff.value);

            persistStatistics();
        }

        private void persistStatistics() throws HyracksDataException {
            List<WaveletCoefficient<Integer, Double>> persistCoefficients = new ArrayList<>(coefficients);
            // sorting coefficients according their keys
            Collections.sort(persistCoefficients, new Comparator<WaveletCoefficient<Integer, Double>>() {
                @Override
                public int compare(WaveletCoefficient<Integer, Double> o1, WaveletCoefficient<Integer, Double> o2) {
                    return o1.getKey().compareTo(o2.getKey());
                }
            });
            Iterator<WaveletCoefficient<Integer, Double>> it = persistCoefficients.iterator();
            int currentPageId = 1;
            while (currentPageId <= numPages) {
                ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), true);
                ByteBuffer buffer = page.getBuffer();
                page.acquireWriteLatch();
                try {
                    while (it.hasNext() && (buffer.limit() - buffer.position()) >= 4 + 8) {
                        WaveletCoefficient<Integer, Double> coeff = it.next();
                        buffer.putInt(coeff.getKey());
                        buffer.putDouble(coeff.getValue());
                    }
                } finally {
                    page.releaseWriteLatch(true);
                    bufferCache.unpin(page);
                }
                ++currentPageId;
            }

        }

    }

}
