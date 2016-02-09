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
package org.apach.hyracks.storage.am.statistics.historgram;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IOrdinalPrimitiveValueProvider;
import org.apache.hyracks.storage.am.common.api.ISynopsis;
import org.apache.hyracks.storage.am.common.api.ISynopsis.SynopsisType;
import org.apache.hyracks.storage.am.common.api.ISynopsisBuilder;
import org.apache.hyracks.storage.am.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsisBuilder;
import org.apache.hyracks.storage.am.statistics.common.StatisticsCollector;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class HistogramCollector extends StatisticsCollector {

    private final SynopsisType statsType;

    public HistogramCollector(IBufferCache bufferCache, IFileMapProvider fileMapProvider, FileReference file,
            int[] fields, int size, ITypeTraits[] fieldTypeTraits, IOrdinalPrimitiveValueProvider fieldValueProvider,
            SynopsisType statsType) throws HyracksDataException {
        super(bufferCache, fileMapProvider, file, fields, size, fieldTypeTraits, fieldValueProvider);
        this.statsType = statsType;
    }

    @Override
    public ISynopsisBuilder createSynopsisBuilder(long numElements) throws HyracksDataException {
        return new HistogramBuilder(numElements);
    }

    public class HistogramBuilder extends AbstractSynopsisBuilder {

        private final EquiHeightHistogramSynopsis<? extends HistogramBucket> histogram;
        private int activeBucket;
        private int addedElementsNum;
        private long lastAddedTuplePosition;
        private final long elementsPerBucket;

        public HistogramBuilder(long numElements) throws HyracksDataException {
            elementsPerBucket = Math.max(numElements / size, 1);
            switch (statsType) {
                case UniformHistogram:
                    histogram = new UniformHistogramSynopsis(fieldTypeTrait, size);
                    break;
                case ContinuousHistogram:
                    histogram = new ContinuousHistogramSynopsis(fieldTypeTrait, size);
                    break;
                default:
                    throw new HyracksDataException("Unsupported histogram type " + statsType);
            }
            activeBucket = 0;
            addedElementsNum = 0;
            lastAddedTuplePosition = histogram.getDomainStart();
        }

        @Override
        public ISynopsis<? extends ISynopsisElement> getSynopsis() {
            return histogram;
        }

        @Override
        public void add(ITupleReference tuple) throws IndexException, HyracksDataException {
            // TODO: what to do with animatter entries?
            long currTuplePosition = fieldValueProvider.getOrdinalValue(tuple.getFieldData(field),
                    tuple.getFieldStart(field));
            if (currTuplePosition != lastAddedTuplePosition && addedElementsNum > elementsPerBucket) {
                histogram.setBucketBorder(activeBucket, currTuplePosition - 1);
                activeBucket++;
                addedElementsNum = 0;
            }
            histogram.appendToBucket(activeBucket, currTuplePosition, 1.0);
            addedElementsNum++;
            lastAddedTuplePosition = currTuplePosition;
        }

        @Override
        public void end() throws IndexException, HyracksDataException {
            histogram.setBucketBorder(activeBucket, histogram.getDomainEnd());
            persistHistogram();
        }

        @Override
        public void abort() throws HyracksDataException {
        }

        private void persistHistogram() {
            // TODO Auto-generated method stub

        }

    }

}
