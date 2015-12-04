package org.apache.hyracks.storage.am.statistics.wavelet;

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
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.api.NumericPointable;
import org.apache.hyracks.data.std.api.OrdinalPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.statistics.common.Synopsis;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class WaveletSynopsis
        /*<K extends IMath<K>, V extends IMath<V>>*/ extends Synopsis<OrdinalPointable, NumericPointable> {

    private final static int METADATA_PAGE_ID = 0;
    private final static int NUM_PAGES_OFFSET = 0;
    private final static int NUM_ELEMENTS_OFFSET = NUM_PAGES_OFFSET + 4;

    private final int[] waveletFields;
    //private final IPrimitiveIntegerValueProvider waveletFieldValueProvider;
    private final IPointableFactory<? extends OrdinalPointable> keyPointableFactory;
    private final IPointableFactory<? extends NumericPointable> valuePointableFactory;

    private final PriorityQueue<WaveletCoefficient<OrdinalPointable, NumericPointable>> coefficients;
    private final long threshold;

    private final int numPages;

    public WaveletSynopsis(IBufferCache bufferCache, IFileMapProvider fileMapProvider, FileReference file,
            int[] keyFields, int threshold, IPointableFactory<? extends OrdinalPointable> keyPointableFactory,
            IPointableFactory<? extends NumericPointable> valuePointableFactory) {
        super(bufferCache, fileMapProvider, file);
        this.waveletFields = keyFields;
        this.keyPointableFactory = keyPointableFactory;
        this.valuePointableFactory = valuePointableFactory;
        this.threshold = threshold;
        this.coefficients = new PriorityQueue<>(threshold);
        this.numPages = (int) Math.ceil(threshold * (4 + 8) / (double) bufferCache.getPageSize());
    }

    @Override
    public int getNumPages() throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("The synopsis is not activated.");
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
    public void addElement(OrdinalPointable index, NumericPointable value) {
        WaveletCoefficient<OrdinalPointable, NumericPointable> newCoeff;
        if (coefficients.size() < threshold)
            newCoeff = new WaveletCoefficient<OrdinalPointable, NumericPointable>(value, 0, index);
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
        private final Stack<WaveletCoefficient<OrdinalPointable, NumericPointable>> avgStack;
        private final Pair<OrdinalPointable, NumericPointable> curr = Pair.of(keyPointableFactory.createPointable(),
                valuePointableFactory.createPointable());
        private long transformPos;
        private final long domainEnd;
        private int lastLevel;
        private final int maxLevel;
        private final List<Pair<OrdinalPointable, NumericPointable>> borderTuples;
        private final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();

        public SparseTransformBuilder() throws HyracksDataException {
            avgStack = new Stack<>();
            borderTuples = new ArrayList<>(2);
            // set initial transform position to minimal value for key doimain
            transformPos = curr.getKey().minDomainValue();
            domainEnd = curr.getKey().maxDomainValue();

            DoubleSerializerDeserializer.INSTANCE.serialize(1.0, abvs.getDataOutput());
            curr.getRight().set(abvs.getByteArray(), bytesWritten, abvs.getLength() - bytesWritten);
            bytesWritten = abvs.getLength();

            maxLevel = curr.getKey().maxLevel();
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

        // method computes number of levels (log base 2 of the distance) between two points
        private int computeLogDistance(long x, long y) {
            return (int) Math.floor(Math.log(x - y + 1) / Math.log(2));
        }

        @Override
        public void add(ITupleReference tuple) throws IndexException, HyracksDataException {
            //            if (waveletFields.length > 1)
            //                throw new HyracksDataException("Wavelet synopsis does not support composite keys");
            curr.getKey().set(tuple.getFieldData(waveletFields[0]), tuple.getFieldStart(waveletFields[0]),
                    tuple.getFieldLength(waveletFields[0]));

            while (transformPos.compareTo(curr.getKey()) <= 0) {
                // current position is a left border of dyadic range
                if (curr.getKey().longValue() == transformPos) {
                    borderTuples.add(Pair.of(0, (double) curr.getValue()));
                    return;
                }
                int newLevel = computeLogDistance(curr.getKey().longValue(), transformPos);
                long levelRightBorder = 1l << newLevel;
                //add first dummy average
                if (avgStack.isEmpty()) {
                    avgStack.push(new WaveletCoefficient<OrdinalPointable, NumericPointable>(0.0, maxLevel, 0));
                    lastLevel = newLevel;
                }

                // current position is a right border of dyadic range
                if (curr.getKey() == transformPos + levelRightBorder - 1 /*&& curr.position != domainMax*/)
                    borderTuples.add(Pair.of((int) (levelRightBorder - 1), (double) curr.getValue()));

                WaveletCoefficient<OrdinalPointable, NumericPointable> newCoeff;
                WaveletCoefficient<OrdinalPointable, NumericPointable> topCoeff = avgStack.peek();
                if (newLevel >= lastLevel) {
                    topCoeff = computeDyadicRange(lastLevel, maxLevel, topCoeff, borderTuples);
                    newCoeff = topCoeff;
                    do {
                        WaveletCoefficient<OrdinalPointable, NumericPointable> oldCoeff = avgStack.pop();
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

        private WaveletCoefficient<OrdinalPointable, NumericPointable> average(
                WaveletCoefficient<OrdinalPointable, NumericPointable> oldCoeff,
                WaveletCoefficient<OrdinalPointable, NumericPointable> newCoeff, int maxLevel) {
            Integer coeffIdx = oldCoeff.index >> 1;
            addElement(coeffIdx, (oldCoeff.value - newCoeff.value)
                    / (2.0 * WaveletCoefficient.getNormalizationCoefficient(maxLevel, oldCoeff.level + 1)));
            WaveletCoefficient<Integer, Double> topCoeff = new WaveletCoefficient<Integer, Double>(
                    (oldCoeff.value + newCoeff.value) / 2.0, oldCoeff.level + 1, coeffIdx);
            return topCoeff;
        }

        private WaveletCoefficient<OrdinalPointable, NumericPointable> computeDyadicRange(int level, int maxLevel,
                WaveletCoefficient<OrdinalPointable, NumericPointable> topCoeff,
                List<Pair<Integer, Double>> borderTuples) {
            //short circuit coefficient computation for 0
            if (borderTuples.isEmpty()) {
                Integer coeffIdx = ((topCoeff.index + 1) << (topCoeff.level - level));
                return new WaveletCoefficient<OrdinalPointable, NumericPointable>(0.0, level, coeffIdx);
            }

            Map<OrdinalPointable, NumericPointable> newCoefs = new HashMap<>();
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

                for (Entry<OrdinalPointable, NumericPointable> e : newCoefs.entrySet())
                    addElement(e.getKey(), e.getValue() / WaveletCoefficient.getNormalizationCoefficient(maxLevel, i));
            }
            return new WaveletCoefficient<OrdinalPointable, NumericPointable>(avg, level, coeffIdx);
        }

        @Override
        public void end() throws IndexException, HyracksDataException {
            //assert(avgStack.size() == 1);
            if (curr.getKey() != domainEnd) {

            }
            WaveletCoefficient<OrdinalPointable, NumericPointable> topCoeff = avgStack.pop();
            topCoeff.index = 0;
            addElement(topCoeff.index, topCoeff.value);

            persistStatistics();
        }

        private void persistStatistics() throws HyracksDataException {
            List<WaveletCoefficient<OrdinalPointable, NumericPointable>> persistCoefficients = new ArrayList<>(
                    coefficients);
            // sorting coefficients according their keys
            Collections.sort(persistCoefficients,
                    new Comparator<WaveletCoefficient<OrdinalPointable, NumericPointable>>() {
                        @Override
                        public int compare(WaveletCoefficient<OrdinalPointable, NumericPointable> o1,
                                WaveletCoefficient<OrdinalPointable, NumericPointable> o2) {
                            return o1.getKey().compareTo(o2.getKey());
                        }
                    });
            Iterator<WaveletCoefficient<OrdinalPointable, NumericPointable>> it = persistCoefficients.iterator();
            int currentPageId = 1;
            while (currentPageId <= numPages) {
                ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), true);
                ByteBuffer buffer = page.getBuffer();
                page.acquireWriteLatch();
                try {
                    while (it.hasNext() && (buffer.limit() - buffer.position()) >= 4 + 8) {
                        WaveletCoefficient<OrdinalPointable, NumericPointable> coeff = it.next();
                        buffer.put(coeff.getKey().getByteArray(), coeff.getKey().getStartOffset(),
                                coeff.getKey().getLength());
                        buffer.put(coeff.getValue().getByteArray(), coeff.getValue().getStartOffset(),
                                coeff.getValue().getLength());
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
