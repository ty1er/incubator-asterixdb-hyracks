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

public class ContinuousHistogramSynopsis extends EquiHeightHistogramSynopsis<HistogramBucket> {

    private static final long serialVersionUID = 1L;

    public ContinuousHistogramSynopsis(ITypeTraits keyTypeTraits, int bucketNum) throws HyracksDataException {
        this(keyTypeTraits, bucketNum, new ArrayList<HistogramBucket>(bucketNum));
    }

    public ContinuousHistogramSynopsis(ITypeTraits keyTypeTraits, int bucketNum, List<HistogramBucket> buckets)
            throws HyracksDataException {
        super(keyTypeTraits, bucketNum, buckets);
    }

    @Override
    public SynopsisType getType() {
        return SynopsisType.ContinuousHistogram;
    }

    public void appendToBucket(int bucketId, long tuplePos, double frequency) {
        if (bucketId >= buckets.size()) {
            buckets.add(new HistogramBucket(0l, frequency));
        } else {
            buckets.get(bucketId).appendToValue(frequency);
        }
    }

    @Override
    public Double pointQuery(Long position) {
        int idx = getPointBucket(position);
        return buckets.get(idx).getValue() / getBucketSpan(idx);
    }

    @Override
    public Double rangeQuery(Long startPosition, Long endPosition) {
        int startBucket = getPointBucket(startPosition);
        int endBucket = getPointBucket(endPosition);
        double value = 0.0;
        if (startBucket == endBucket) {
            value = buckets.get(startBucket).getValue() * ((double) endPosition - startPosition)
                    / getBucketSpan(startBucket);
        } else {
            //account for part of the initial bucket between startPosition and it's right border
            value += buckets.get(startBucket).getValue() * ((double) buckets.get(startBucket).getKey() - startPosition)
                    / getBucketSpan(startBucket);
            //...and for the part between left border of the last bucket and endPosition
            value += buckets.get(endBucket).getValue() * ((double) endPosition - buckets.get(endBucket - 1).getKey())
                    / getBucketSpan(endBucket);
            //sum up all the buckets in between
            for (int i = startBucket + 1; i < endBucket - 1; i++) {
                value += buckets.get(i).getValue();
            }
        }
        return value;
    }

}
