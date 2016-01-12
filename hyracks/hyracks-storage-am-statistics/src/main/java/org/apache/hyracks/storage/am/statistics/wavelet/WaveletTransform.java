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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.storage.am.common.api.IOrdinalPrimitiveValueProvider;
import org.apache.hyracks.storage.am.common.api.ISynopsis;
import org.apache.hyracks.storage.am.common.api.ISynopsisBuilder;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.statistics.common.StatisticsCollector;
import org.apache.hyracks.storage.am.statistics.common.TypeTraitsDomainUtils;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.file.IFileMapProvider;
import org.apache.hyracks.util.objectpool.IObjectFactory;
import org.apache.hyracks.util.objectpool.MapObjectPool;

public class WaveletTransform extends StatisticsCollector {

    private final static int METADATA_PAGE_ID = 0;
    private final static int NUM_PAGES_OFFSET = 0;
    private final static int NUM_ELEMENTS_OFFSET = NUM_PAGES_OFFSET + Integer.BYTES;
    private final static int SYNOPSIS_KEY_SIZE = Long.BYTES;
    private final static int SYNOPSIS_VALUE_SIZE = Double.BYTES;

    private final int[] fields;
    private final IOrdinalPrimitiveValueProvider fieldValueProvider;
    private final ITypeTraits[] fieldTypeTraits;
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer synopsisKeySerde = Integer64SerializerDeserializer.INSTANCE;
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer synopsisValueSerde = DoubleSerializerDeserializer.INSTANCE;

    private final WaveletSynopsis synopsis;

    private transient final int numPages;

    public WaveletTransform(IBufferCache bufferCache, IFileMapProvider fileMapProvider, FileReference file,
            int[] fields, int threshold, ITypeTraits[] fieldTypeTraits,
            IOrdinalPrimitiveValueProvider fieldValueProvider) {
        super(bufferCache, fileMapProvider, file);
        this.fields = fields;
        this.fieldValueProvider = fieldValueProvider;
        this.fieldTypeTraits = fieldTypeTraits;
        this.numPages = (int) Math
                .ceil(threshold * (SYNOPSIS_KEY_SIZE + SYNOPSIS_VALUE_SIZE) / (double) bufferCache.getPageSize());
        this.synopsis = new WaveletSynopsis(threshold);
    }

    @Override
    public ISynopsis getSynopsis() {
        return synopsis;
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        super.create();

        initWaveletSynopsisMetaData();
        bufferCache.closeFile(fileId);
    }

    @Override
    public synchronized void deactivate() throws HyracksDataException {
        super.deactivate();
    }

    public int getNumPages() throws HyracksDataException {
        if (!isActive) {
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

    // Appends value to the coefficient with given index. If such coefficient is not found creates a new coeff
    public void apendToElement(long index, double appendValue) {
        // TODO: do something better than linear search
        for (Map.Entry<Long, Double> coeff : synopsis) {
            if (coeff.getKey() == index) {
                coeff.setValue(coeff.getValue() + appendValue);
                return;
            }
        }
        synopsis.addElement(index, appendValue);
    }

    @Override
    public ISynopsisBuilder createSynopsisBuilder() throws HyracksDataException {
        return new SparseWaveletTransformBuilder();
    }

    public class SparseWaveletTransformBuilder implements ISynopsisBuilder {
        private final Stack<WaveletCoefficient> avgStack;
        private MapObjectPool<WaveletCoefficient, Integer> avgStackObjectPool;
        private final long domainEnd;
        private final long domainStart;
        private final int maxLevel;
        private Long prevPosition;
        private ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        private boolean isAntimatterTuple = false;

        public SparseWaveletTransformBuilder() throws HyracksDataException {
            if (fields.length > 1) {
                throw new HyracksDataException("Unable to collect statistics on composite keys");
            }
            if (!fieldTypeTraits[fields[0]].isFixedLength() || fieldTypeTraits[fields[0]].getFixedLength() > 9) {
                throw new HyracksDataException(
                        "Unable to collect statistics for key field with typeTrait" + fieldTypeTraits[fields[0]]);
            }
            domainStart = TypeTraitsDomainUtils.minDomainValue(fieldTypeTraits[fields[0]]);
            domainEnd = TypeTraitsDomainUtils.maxDomainValue(fieldTypeTraits[fields[0]]);
            maxLevel = TypeTraitsDomainUtils.maxLevel(fieldTypeTraits[fields[0]]);

            avgStack = new Stack<>();
            avgStackObjectPool = new MapObjectPool<WaveletCoefficient, Integer>();
            IObjectFactory<WaveletCoefficient, Integer> waveletFactory = new IObjectFactory<WaveletCoefficient, Integer>() {
                @Override
                public WaveletCoefficient create(Integer level) {
                    return new WaveletCoefficient(0.0, level, -1);
                }
            };
            for (int i = -1; i <= maxLevel; i++) {
                avgStackObjectPool.register(i, waveletFactory);
            }
            //add first dummy average
            WaveletCoefficient dummyCoeff = avgStackObjectPool.allocate(-1);
            dummyCoeff.setIndex(-1);
            avgStack.push(dummyCoeff);
            prevPosition = null;

            persistWaveletSynopsisMetaData();
        }

        @Override
        public void setAntimatterTuple(boolean isAntimatter) {
            this.isAntimatterTuple = isAntimatter;
        }

        private void persistWaveletSynopsisMetaData() throws HyracksDataException {
            ICachedPage metaPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, METADATA_PAGE_ID), false);
            metaPage.acquireWriteLatch();
            try {
                metaPage.getBuffer().putInt(NUM_PAGES_OFFSET, numPages);
                metaPage.getBuffer().putLong(NUM_ELEMENTS_OFFSET, synopsis.size());
            } finally {
                metaPage.releaseWriteLatch(true);
                bufferCache.unpin(metaPage);
            }
        }

        // Modifies the wavelet coefficients in case when tuple was already transformed
        private void modifyTuple(WaveletCoefficient topCoeff, double tupleValue) {
            // the tuple on the top is always right end of dyadic range
            long rightCoeffId = topCoeff.getKey();
            for (long i = topCoeff.getLevel(); i > 0; i--) {
                // update coefficients, corresponding to all subranges having current position as they right end
                apendToElement(rightCoeffId, (i == 0 ? 1 : -1) * tupleValue / (1l << i));
                rightCoeffId = rightCoeffId << 1 | 1;
            }
            // put modified top coefficient back to the stack
            topCoeff.setValue(topCoeff.getValue() + tupleValue / (1l << topCoeff.getLevel()));
            avgStack.push(topCoeff);

        }

        // Returns the parent wavelet coefficient for a given coefficient in the transform tree
        private WaveletCoefficient moveLevelUp(WaveletCoefficient childCoeff) {
            WaveletCoefficient parentCoeff = avgStackObjectPool.allocate(childCoeff.getLevel() + 1);
            parentCoeff.setValue(childCoeff.getValue() / 2.0);
            parentCoeff.setIndex(childCoeff.getParentCoeffIndex(domainStart, maxLevel));
            return parentCoeff;
        }

        // Calculates the position of the next tuple (on level 0) after given wavelet coefficient
        private long getTransformPosition(WaveletCoefficient coeff) {
            if (coeff.getLevel() < 0) {
                return domainStart;
            } else if (coeff.getLevel() == 0) {
                return coeff.getKey() + 1;
            } else {
                return ((((coeff.getKey() + 1) << (coeff.getLevel() - 1)) - (1l << (maxLevel - 1))) << 1) + domainStart;
            }
        }

        // Combines two coeffs on the same level by averaging them and producing next level coefficient
        private void average(WaveletCoefficient leftCoeff, WaveletCoefficient rightCoeff, long domainMin, int maxLevel,
                boolean normalize, WaveletCoefficient avgCoeff) {
            //        assert (leftCoeff.getLevel() == rightCoeff.getLevel());
            long coeffIdx = leftCoeff.getParentCoeffIndex(domainMin, maxLevel);
            // put detail wavelet coefficient to the coefficient queue
            synopsis.addElement(coeffIdx, (leftCoeff.getValue() - rightCoeff.getValue()) / (2.0 * (normalize
                    ? WaveletCoefficient.getNormalizationCoefficient(maxLevel, leftCoeff.getLevel() + 1) : 1)));
            avgCoeff.setIndex(coeffIdx);
            avgCoeff.setValue((leftCoeff.getValue() + rightCoeff.getValue()) / 2.0);
        }

        // Pushes given coefficient on the stack, possibly triggering domino effect
        private void pushToStack(WaveletCoefficient newCoeff) {
            // if the coefficient on the top of the stack has the same level as new coefficient, they should be combined
            while (!avgStack.isEmpty() && avgStack.peek().getLevel() == newCoeff.getLevel()) {
                WaveletCoefficient topCoeff = avgStack.pop();
                // Guard against dummy coefficients
                if (topCoeff.getLevel() >= 0) {
                    //allocate next level coefficient from objectPool
                    WaveletCoefficient avgCoeff = avgStackObjectPool.allocate(topCoeff.getLevel() + 1);
                    // combine newCoeff and topCoeff by averaging them. Result coeff's level is greater than parent's level by 1
                    average(topCoeff, newCoeff, domainStart, maxLevel, true, avgCoeff);
                    avgStackObjectPool.deallocate(topCoeff.getLevel(), topCoeff);
                    avgStackObjectPool.deallocate(newCoeff.getLevel(), newCoeff);
                    newCoeff = avgCoeff;
                }
            }
            // Guard against dummy coefficients
            if (newCoeff.getLevel() >= 0) {
                avgStack.push(newCoeff);
            }
        }

        private void transformTuple(WaveletCoefficient topCoeff, long tuplePosition, double tupleValue) {
            // 1st part: Upward transform
            WaveletCoefficient newCoeff = moveLevelUp(topCoeff);
            // Move the current top coefficient 1 level up as far as possible (until it will cover current position)
            while (!newCoeff.covers(tuplePosition, maxLevel, domainStart)
                    && (avgStack.size() > 0 ? avgStack.peek().getLevel() > (newCoeff.getLevel() - 1) : true)
                    && topCoeff.getLevel() >= 0) {
                topCoeff = newCoeff;
                synopsis.addElement(newCoeff.getKey(), newCoeff.getValue());
                newCoeff = moveLevelUp(newCoeff);
            }
            avgStackObjectPool.deallocate(newCoeff.getLevel(), newCoeff);
            newCoeff = topCoeff;
            // put the top coefficient (possibly modified) back on to the stack
            pushToStack(newCoeff);

            // 2nd part: Downward transform
            if (avgStack.size() > 0) {
                newCoeff = avgStack.peek();
            }
            // calculate the tuple position, where the transform currently stopped
            long transformPosition = getTransformPosition(newCoeff);
            // put all the coefficients, corresponding to dyadic ranges between current tuple position & transformPosition on the stack
            computeDyadicSubranges(tuplePosition, transformPosition);
            // put the last coefficient, corresponding to current tuple position on to the stack
            newCoeff = avgStackObjectPool.allocate(0);
            newCoeff.setValue(tupleValue);
            newCoeff.setIndex(tuplePosition);
            pushToStack(newCoeff);
        }

        // Method calculates decreasing level dyadic intervals between tuplePosition&currTransformPosition and saves corresponding coefficients in the avgStack
        private void computeDyadicSubranges(long tuplePosition, long currTransformPosition) {
            while (tuplePosition != currTransformPosition) {
                WaveletCoefficient coeff;
                if (avgStack.size() > 0) {
                    coeff = avgStackObjectPool.allocate(avgStack.peek().getLevel());
                    coeff.setValue(0.0);
                    // starting with the sibling of the top coefficient on the stack
                    coeff.setIndex(avgStack.peek().getKey() + 1l);
                }
                // special case when there is no coeffs on the stack.
                else {
                    coeff = avgStackObjectPool.allocate(maxLevel);
                    coeff.setValue(0.0);
                    // Starting descent from top coefficient, i.e. the one with index == 1, level == maxLevel
                    coeff.setIndex(1l);
                }
                // decrease the coefficient level until it stops covering tuplePosition
                while (coeff.covers(tuplePosition, maxLevel, domainStart)) {
                    avgStackObjectPool.deallocate(coeff.getLevel(), coeff);
                    WaveletCoefficient newCoeff = avgStackObjectPool.allocate(coeff.getLevel() - 1);
                    if (newCoeff.getLevel() == 0) {
                        newCoeff.setIndex(((coeff.getKey() - (1l << (maxLevel - 1))) << 1) + domainStart);
                    } else {
                        newCoeff.setIndex(coeff.getKey() << 1);
                    }
                    coeff = newCoeff;
                }
                // we don't add newCoeff to the wavelet coefficient collection, since it's value is 0. Keep it only in average stack
                pushToStack(coeff);
                currTransformPosition = getTransformPosition(coeff);
            }
        }

        @Override
        public void add(ITupleReference tuple) throws IndexException, HyracksDataException {

            WaveletCoefficient topCoeff = avgStack.pop();
            boolean neg = false;
            if (isAntimatterTuple) {
                neg = ((ILSMTreeTupleReference) tuple).isAntimatter();
            }
            long currTuplePosition = fieldValueProvider.getOrdinalValue(tuple.getFieldData(fields[0]),
                    tuple.getFieldStart(fields[0]));
            double currTupleValue = neg ? -1.0 : 1.0;

            // check whether tuple with this position was already seen
            if (prevPosition != null && currTuplePosition == prevPosition) {
                modifyTuple(topCoeff, currTupleValue);
            } else {
                transformTuple(topCoeff, currTuplePosition, currTupleValue);
            }
            prevPosition = currTuplePosition;
        }

        @Override
        public void end() throws IndexException, HyracksDataException {
            WaveletCoefficient topCoeff = avgStack.pop();
            if (topCoeff.getKey() > 0) {
                if (prevPosition == null || prevPosition != domainEnd) {
                    //complete transform by submitting dummy tuple with the last position avaiable for given domain
                    transformTuple(topCoeff, domainEnd, 0.0);
                    topCoeff = avgStack.pop();
                }
                //assert(avgStack.size() == 1);
                // now the transform is complete the top coefficient on the stack is global average, i.e. coefficient with index==0
                synopsis.addElement(0l, topCoeff.getValue());

                persistStatistics();
            }
        }

        @SuppressWarnings("unchecked")
        private void persistStatistics() throws HyracksDataException {
            synopsis.sortOnKey();
            Iterator<Map.Entry<Long, Double>> it = synopsis.iterator();
            int currentPageId = 1;
            while (currentPageId <= numPages) {
                ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), true);
                ByteBuffer pageBuffer = page.getBuffer();
                page.acquireWriteLatch();
                try {
                    while (it.hasNext() && (pageBuffer.limit() - pageBuffer.position()) >= SYNOPSIS_KEY_SIZE
                            + SYNOPSIS_VALUE_SIZE) {
                        tupleBuilder.reset();
                        Map.Entry<Long, Double> entry = it.next();
                        tupleBuilder.addField(synopsisKeySerde, entry.getKey());
                        tupleBuilder.addField(synopsisValueSerde, entry.getValue());
                        pageBuffer.put(tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
                    }
                } finally {
                    page.releaseWriteLatch(true);
                    bufferCache.unpin(page);
                }
                ++currentPageId;
            }
        }

        @Override
        public void abort() throws HyracksDataException {
        }

    }

}
