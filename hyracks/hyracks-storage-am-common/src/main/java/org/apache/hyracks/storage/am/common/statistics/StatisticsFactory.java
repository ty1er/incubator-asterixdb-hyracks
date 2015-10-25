package org.apache.hyracks.storage.am.common.statistics;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IPrimitiveIntegerValueProviderFactory;
import org.apache.hyracks.storage.am.common.statistics.wavelet.WaveletSynopsis;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class StatisticsFactory {
    private final IBufferCache bufferCache;
    private final IFileMapProvider fileMapProvider;
    private final int[] statisticsKeyFields;
    private final IPrimitiveIntegerValueProviderFactory valueProviderFactory;

    public StatisticsFactory(IBufferCache bufferCache, IFileMapProvider fileMapProvider, int[] statisticsKeyFields,
            IPrimitiveIntegerValueProviderFactory valueProviderFactory) {
        this.bufferCache = bufferCache;
        this.fileMapProvider = fileMapProvider;
        this.statisticsKeyFields = statisticsKeyFields;
        this.valueProviderFactory = valueProviderFactory;
    }

    public WaveletSynopsis createWaveletStatistics(FileReference file) throws HyracksDataException {
        return new WaveletSynopsis(bufferCache, fileMapProvider, file, statisticsKeyFields, 10, valueProviderFactory);
    }

    public int[] getStatisticsFields() {
        return statisticsKeyFields;
    }
}
