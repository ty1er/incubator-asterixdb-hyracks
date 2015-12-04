package org.apache.hyracks.storage.am.statistics.common;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.api.NumericPointable;
import org.apache.hyracks.data.std.api.OrdinalPointable;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletSynopsis;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class StatisticsFactory {
    private final IBufferCache bufferCache;
    private final IFileMapProvider fileMapProvider;
    private final int[] statisticsKeyFields;
    private final IPointableFactory<? extends OrdinalPointable> keyPointableFactory;
    private final IPointableFactory<? extends NumericPointable> valuePointableFactory;

    public StatisticsFactory(IBufferCache bufferCache, IFileMapProvider fileMapProvider, int[] statisticsKeyFields,
            IPointableFactory<? extends OrdinalPointable> keyPointableFactory,
            IPointableFactory<? extends NumericPointable> valuePointableFactory) {
        this.bufferCache = bufferCache;
        this.fileMapProvider = fileMapProvider;
        this.statisticsKeyFields = statisticsKeyFields;
        this.keyPointableFactory = keyPointableFactory;
        this.valuePointableFactory = valuePointableFactory;
    }

    public WaveletSynopsis createWaveletStatistics(FileReference file) throws HyracksDataException {
        return new WaveletSynopsis(bufferCache, fileMapProvider, file, statisticsKeyFields, 10, keyPointableFactory,
                valuePointableFactory);
    }

    public int[] getStatisticsFields() {
        return statisticsKeyFields;
    }
}
