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

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apach.hyracks.storage.am.statistics.historgram.UniformHistogramBucket;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.storage.am.common.api.IOrdinalPrimitiveValueProvider;
import org.apache.hyracks.storage.am.common.api.ISynopsis;
import org.apache.hyracks.storage.am.common.api.ISynopsisBuilder;
import org.apache.hyracks.storage.am.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.common.impls.AbstractFileManager;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public abstract class StatisticsCollector extends AbstractFileManager {

    protected int size;
    protected final int[] fields;
    protected final IOrdinalPrimitiveValueProvider fieldValueProvider;
    protected final ITypeTraits[] fieldTypeTraits;
    private final int numPages;

    protected final static int METADATA_PAGE_ID = 0;
    protected final static int NUM_PAGES_OFFSET = 0;
    protected final static int NUM_ELEMENTS_OFFSET = NUM_PAGES_OFFSET + Integer.BYTES;

    public StatisticsCollector(IBufferCache bufferCache, IFileMapProvider fileMapProvider, FileReference file,
            int[] fields, int size, ITypeTraits[] fieldTypeTraits, IOrdinalPrimitiveValueProvider fieldValueProvider) {
        super(bufferCache, fileMapProvider, file);
        this.size = size;
        this.numPages = (int) Math
                .ceil(size * (ISynopsisElement.SYNOPSIS_KEY_SIZE + ISynopsisElement.SYNOPSIS_VALUE_SIZE)
                        / (double) bufferCache.getPageSize());
        this.fieldValueProvider = fieldValueProvider;
        this.fields = fields;
        this.fieldTypeTraits = fieldTypeTraits;
    }

    protected void check() throws HyracksDataException {
        if (fields.length > 1) {
            throw new HyracksDataException("Unable to collect statistics on composite keys");
        }
        if (!fieldTypeTraits[fields[0]].isFixedLength() || fieldTypeTraits[fields[0]].getFixedLength() > 9) {
            throw new HyracksDataException(
                    "Unable to collect statistics for key field with typeTrait" + fieldTypeTraits[fields[0]]);
        }
    }

    protected void persistSynopsisMetadata(int pageNum, long elementNum, boolean newPage) throws HyracksDataException {
        ICachedPage metaPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, METADATA_PAGE_ID), newPage);
        metaPage.acquireWriteLatch();
        try {
            metaPage.getBuffer().putInt(NUM_PAGES_OFFSET, pageNum);
            metaPage.getBuffer().putLong(NUM_ELEMENTS_OFFSET, elementNum);
        } finally {
            metaPage.releaseWriteLatch(true);
            bufferCache.unpin(metaPage);
        }
    }

    @SuppressWarnings("unchecked")
    protected void persistSynopsis(ISynopsis<? extends ISynopsisElement> synopsis) throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        Iterator<? extends ISynopsisElement> it = synopsis.iterator();
        int currentPageId = 1;
        while (currentPageId <= getNumPages()) {
            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), true);
            ByteBuffer pageBuffer = page.getBuffer();
            page.acquireWriteLatch();
            try {
                while (it.hasNext()
                        && (pageBuffer.limit() - pageBuffer.position()) >= ISynopsisElement.SYNOPSIS_KEY_SIZE
                                + ISynopsisElement.SYNOPSIS_VALUE_SIZE) {
                    tupleBuilder.reset();
                    ISynopsisElement entry = it.next();
                    tupleBuilder.addField(ISynopsisElement.SYNOPSIS_KEY_SERDE, entry.getKey());
                    tupleBuilder.addField(ISynopsisElement.SYNOPSIS_VALUE_SERDE, entry.getValue());
                    if (entry instanceof UniformHistogramBucket) {
                        tupleBuilder.addField(UniformHistogramBucket.UNIQUE_VALUE_NUM_SERDE,
                                ((UniformHistogramBucket) entry).getUniqueElementsNum());
                    }
                    pageBuffer.put(tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
                }
            } finally {
                page.releaseWriteLatch(true);
                bufferCache.unpin(page);
            }
            ++currentPageId;
        }
    }

    public int getNumPages() throws HyracksDataException {
        if (!isActive) {
            throw new HyracksDataException("The synopsis is not activated.");
        }
        return numPages;
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        super.create();
        //TODO:for now disable local persistence of the stats
        //persistSynopsisMetadata(getNumPages(), size, true);
        bufferCache.closeFile(fileId);
    }

    @Override
    public synchronized void deactivate() throws HyracksDataException {
        super.deactivate();
    }

    public abstract ISynopsisBuilder createSynopsisBuilder(long numElements) throws HyracksDataException;
}
