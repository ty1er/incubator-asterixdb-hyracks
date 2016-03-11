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

import java.util.List;

import org.apach.hyracks.storage.am.statistics.historgram.ContinuousHistogramSynopsis;
import org.apach.hyracks.storage.am.statistics.historgram.HistogramBucket;
import org.apach.hyracks.storage.am.statistics.historgram.UniformHistogramBucket;
import org.apach.hyracks.storage.am.statistics.historgram.UniformHistogramSynopsis;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ISynopsis;
import org.apache.hyracks.storage.am.common.api.ISynopsis.SynopsisType;
import org.apache.hyracks.storage.am.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletCoefficient;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletSynopsis;

public class SynopsisFactory {
    @SuppressWarnings("unchecked")
    public static ISynopsis<? extends ISynopsisElement> createSynopsis(SynopsisType type,
            ITypeTraits keyTypeTraits, List<? extends ISynopsisElement> elements) throws HyracksDataException {
        switch (type) {
            case UniformHistogram:
                return new UniformHistogramSynopsis(keyTypeTraits, elements.size(),
                        (List<UniformHistogramBucket>) elements);
            case ContinuousHistogram:
                return new ContinuousHistogramSynopsis(keyTypeTraits, elements.size(),
                        (List<HistogramBucket>) elements);
            case Wavelet:
                return new WaveletSynopsis(keyTypeTraits, (List<WaveletCoefficient>) elements, elements.size());
            default:
                throw new HyracksDataException("Cannot instanciate new synopsis of type " + type);
        }
    }
}
