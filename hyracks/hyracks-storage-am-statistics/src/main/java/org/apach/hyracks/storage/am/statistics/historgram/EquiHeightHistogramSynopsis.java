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

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ISynopsis;
import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsis;

public abstract class EquiHeightHistogramSynopsis<T extends HistogramBucket> extends AbstractSynopsis<T> {

    private static final long serialVersionUID = 1L;

    protected List<T> buckets;

    public EquiHeightHistogramSynopsis(ITypeTraits keyTypeTraits, int size, List<T> buckets)
            throws HyracksDataException {
        super(keyTypeTraits, size);
        this.buckets = buckets;
    }

    @Override
    public Iterator<T> iterator() {
        return buckets.iterator();
    }

    @Override
    public void merge(ISynopsis<T> mergeSynopsis) {
        throw new UnsupportedOperationException();
    }

    protected long getBucketSpan(int bucketId) {
        long start = getBucketStartPosition(bucketId);
        return buckets.get(bucketId).getKey() - start + 1;
    }

    protected long getBucketStartPosition(int idx) {
        return idx == 0 ? domainStart : buckets.get(idx - 1).getKey() + 1;
    }

    protected int getPointBucket(Long position) {
        int idx = Collections.binarySearch(buckets, new HistogramBucket(position, 0.0),
                new Comparator<HistogramBucket>() {
                    @Override
                    public int compare(HistogramBucket o1, HistogramBucket o2) {
                        return o1.getKey().compareTo(o2.getKey());
                    }
                });
        if (idx < 0) {
            idx = -idx - 1;
        }
        return idx;
    }

    public void setBucketBorder(int bucket, long border) {
        buckets.get(bucket).setRightBorder(border);
    }

    public abstract void appendToBucket(int bucketId, long tuplePos, double frequency);
}
