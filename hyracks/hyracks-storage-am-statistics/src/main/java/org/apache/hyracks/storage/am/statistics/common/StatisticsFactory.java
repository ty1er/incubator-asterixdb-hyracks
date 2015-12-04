package org.apache.hyracks.storage.am.statistics.common;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IOrdinalPrimitiveValueProvider;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletSynopsis;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class StatisticsFactory {
    private final IBufferCache bufferCache;
    private final IFileMapProvider fileMapProvider;
    private final int[] statsFields;
    private final ITypeTraits[] statsFieldTypeTraits;
    private final IOrdinalPrimitiveValueProvider statsFieldValueProvider;

    public StatisticsFactory(IBufferCache bufferCache, IFileMapProvider fileMapProvider, int[] statsFields,
            ITypeTraits[] statsFieldTypeTraits, IOrdinalPrimitiveValueProvider statsFieldValueProvider) {
        this.bufferCache = bufferCache;
        this.fileMapProvider = fileMapProvider;
        this.statsFields = statsFields;
        this.statsFieldTypeTraits = statsFieldTypeTraits;
        this.statsFieldValueProvider = statsFieldValueProvider;
    }

    public WaveletSynopsis createWaveletStatistics(FileReference file) throws HyracksDataException {
        return new WaveletSynopsis(bufferCache, fileMapProvider, file, statsFields, 10, statsFieldTypeTraits,
                statsFieldValueProvider);
    }
}
