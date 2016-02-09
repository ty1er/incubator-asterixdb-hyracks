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

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class UniformHistogramSynopsis extends EquiHeightHistogramSynopsis<UniformHistogramBucket> {

    private static final long serialVersionUID = 1L;
    private long lastAppendedTuple;

    public UniformHistogramSynopsis(ITypeTraits keyTypeTraits, int size, List<UniformHistogramBucket> buckets)
            throws HyracksDataException {
        super(keyTypeTraits, size, buckets);
    }

    public UniformHistogramSynopsis(ITypeTraits keyTypeTraits, int bucketNum) throws HyracksDataException {
        this(keyTypeTraits, bucketNum, new ArrayList<UniformHistogramBucket>(bucketNum));
    }

    @Override
    public void appendToBucket(int bucketId, long tuple, double frequency) {
        if (bucketId >= buckets.size()) {
            lastAppendedTuple = tuple;
            buckets.add(new UniformHistogramBucket(0l, frequency, 1));
        } else {
            if (tuple != lastAppendedTuple) {
                lastAppendedTuple = tuple;
                buckets.get(bucketId).incUniqueElementsNum();
            }
            buckets.get(bucketId).appendToValue(frequency);
        }
    }

    @Override
    public SynopsisType getType() {
        return SynopsisType.UniformHistogram;
    }

    @Override
    public Double pointQuery(Long position) {
        int idx = getPointBucket(position);
        long bucketStart = getBucketStartPosition(idx);
        return getUniformValue(idx, position - bucketStart, position - bucketStart);
    }

    public double getUniformValue(int bucketIdx, long start, long end) {
        double value = 0.0;
        long k = (long) Math.floor((double) getBucketSpan(bucketIdx) / buckets.get(bucketIdx).getUniqueElementsNum());
        for (long i = start; i <= end; i++) {
            if (i % k == 0) {
                value += buckets.get(bucketIdx).getValue() / buckets.get(bucketIdx).getUniqueElementsNum();
            }
        }
        return value;
    }

    @Override
    public Double rangeQuery(Long startPosition, Long endPosition) {
        int startBucket = getPointBucket(startPosition);
        long startBucketLeftBorder = getBucketStartPosition(startBucket);
        int endBucket = getPointBucket(endPosition);
        long endBucketLeftBorder = getBucketStartPosition(endBucket);
        double value = 0.0;
        if (startBucket == endBucket) {
            getUniformValue(startBucket, startPosition - endBucketLeftBorder, endPosition - endBucketLeftBorder);
        } else {
            //account for part of the initial bucket between startPosition and it's right border
            value += getUniformValue(startBucket, startPosition - startBucketLeftBorder,
                    buckets.get(startBucket).getKey() - startBucketLeftBorder);
            //...and for the part between left border of the last bucket and endPosition
            value += getUniformValue(startBucket, 0, endPosition - endBucketLeftBorder);
            //sum up all the buckets in between
            for (int i = startBucket + 1; i < endBucket - 1; i++) {
                value += buckets.get(i).getValue();
            }
        }
        return value;
    }

}
