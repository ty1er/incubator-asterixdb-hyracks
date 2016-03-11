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

package org.apache.hyracks.storage.am.statistics.common;

import org.apach.hyracks.storage.am.statistics.historgram.HistogramCollector;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IOrdinalPrimitiveValueProvider;
import org.apache.hyracks.storage.am.common.api.ISynopsis.SynopsisType;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletTransform;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class StatisticsCollectorFactory {
    private final IBufferCache bufferCache;
    private final IFileMapProvider fileMapProvider;
    private final int[] statsFields;
    private final int statsSize;
    private final ITypeTraits[] statsFieldTypeTraits;
    private final SynopsisType statsType;
    private final IOrdinalPrimitiveValueProvider statsFieldValueProvider;

    public StatisticsCollectorFactory(IBufferCache bufferCache, IFileMapProvider fileMapProvider,
            SynopsisType statsType, int[] statsFields, int statsSize, ITypeTraits[] statsFieldTypeTraits,
            IOrdinalPrimitiveValueProvider statsFieldValueProvider) {
        this.bufferCache = bufferCache;
        this.fileMapProvider = fileMapProvider;
        this.statsType = statsType;
        this.statsFields = statsFields;
        this.statsSize = statsSize;
        this.statsFieldTypeTraits = statsFieldTypeTraits;
        this.statsFieldValueProvider = statsFieldValueProvider;
    }

    public StatisticsCollector createStatisticsCollector(FileReference file) throws HyracksDataException {
        switch (statsType) {
            case ContinuousHistogram:
            case UniformHistogram:
                return createHistorgamStatistics(file);
            case Wavelet:
                return createWaveletStatistics(file);
            default:
                throw new HyracksDataException("Cannot instanciate new synopsis collector for type " + statsType);
        }
    }

    private StatisticsCollector createWaveletStatistics(FileReference file) {
        return new WaveletTransform(bufferCache, fileMapProvider, file, statsFields, statsSize, statsFieldTypeTraits,
                statsFieldValueProvider);
    }

    public StatisticsCollector createHistorgamStatistics(FileReference file) {
        return new HistogramCollector(bufferCache, fileMapProvider, file, statsFields, statsSize, statsFieldTypeTraits,
                statsFieldValueProvider, statsType);
    }
}
