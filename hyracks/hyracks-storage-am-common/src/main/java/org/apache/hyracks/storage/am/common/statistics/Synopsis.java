package org.apache.hyracks.storage.am.common.statistics;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;
import org.apache.hyracks.storage.am.common.impls.AbstractFileManager;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public abstract class Synopsis<K, V> extends AbstractFileManager {

    public Synopsis(IBufferCache bufferCache, IFileMapProvider fileMapProvider, FileReference file) {
        super(bufferCache, fileMapProvider, file);
    }

    public abstract void addElement(K key, V value);

    public abstract IIndexBulkLoader createBuilder() throws HyracksDataException;

    public abstract int getNumPages() throws HyracksDataException;

}
