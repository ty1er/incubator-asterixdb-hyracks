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

package org.apache.hyracks.storage.am.bloomfilter.impls;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.impls.AbstractFileManager;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageQueue;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class BloomFilter extends AbstractFileManager {

    private final static int METADATA_PAGE_ID = 0;
    private final static int NUM_PAGES_OFFSET = 0; // 0
    private final static int NUM_HASHES_USED_OFFSET = NUM_PAGES_OFFSET + 4; // 4
    private final static int NUM_ELEMENTS_OFFSET = NUM_HASHES_USED_OFFSET + 4; // 8
    private final static int NUM_BITS_OFFSET = NUM_ELEMENTS_OFFSET + 8; // 12

    private final int[] keyFields;

    private int numPages;
    private int numHashes;
    private long numElements;
    private long numBits;
    private final int numBitsPerPage;
    private final static byte[] ZERO_BUFFER = new byte[131072]; // 128kb
    private final static long SEED = 0L;

    public BloomFilter(IBufferCache bufferCache, IFileMapProvider fileMapProvider, FileReference file, int[] keyFields)
            throws HyracksDataException {
        super(bufferCache, fileMapProvider, file);
        this.keyFields = keyFields;
        this.numBitsPerPage = bufferCache.getPageSize() * Byte.SIZE;
    }

    public int getNumPages() throws HyracksDataException {
        if (!isActive) {
            activate();
        }
        return numPages;
    }

    public long getNumElements() throws HyracksDataException {
        if (!isActive) {
            throw new HyracksDataException("The bloom filter is not activated.");
        }
        return numElements;
    }

    public boolean contains(ITupleReference tuple, long[] hashes) throws HyracksDataException {
        if (numPages == 0) {
            return false;
        }
        MurmurHash128Bit.hash3_x64_128(tuple, keyFields, SEED, hashes);
        for (int i = 0; i < numHashes; ++i) {
            long hash = Math.abs((hashes[0] + i * hashes[1]) % numBits);

            // we increment the page id by one, since the metadata page id of the filter is 0.
            ICachedPage page = bufferCache
                    .pin(BufferedFileHandle.getDiskPageId(fileId, (int) (hash / numBitsPerPage) + 1), false);
            page.acquireReadLatch();
            try {
                ByteBuffer buffer = page.getBuffer();
                int byteIndex = (int) (hash % numBitsPerPage) >> 3; // divide by 8
                byte b = buffer.get(byteIndex);
                int bitIndex = (int) (hash % numBitsPerPage) & 0x07; // mod 8

                if (!((b & (1L << bitIndex)) != 0)) {
                    return false;
                }
            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }

        }
        return true;
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        super.create();

        bufferCache.closeFile(fileId);
    }

    @Override
    public synchronized void activate() throws HyracksDataException {
        super.activate();

        readBloomFilterMetaData();
    }

    private void readBloomFilterMetaData() throws HyracksDataException {
        if (bufferCache.getNumPagesOfFile(fileId) == 0) {
            numPages = 0;
            numHashes = 0;
            numElements = 0;
            numBits = 0;
            return;
        }
        ICachedPage metaPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, METADATA_PAGE_ID), false);
        metaPage.acquireReadLatch();
        try {
            numPages = metaPage.getBuffer().getInt(NUM_PAGES_OFFSET);
            numHashes = metaPage.getBuffer().getInt(NUM_HASHES_USED_OFFSET);
            numElements = metaPage.getBuffer().getLong(NUM_ELEMENTS_OFFSET);
            numBits = metaPage.getBuffer().getLong(NUM_BITS_OFFSET);
        } finally {
            metaPage.releaseReadLatch();
            bufferCache.unpin(metaPage);
        }
    }

    public IIndexBulkLoader createBuilder(long numElements, int numHashes, int numBitsPerElement)
            throws HyracksDataException {
        return new BloomFilterBuilder(numElements, numHashes, numBitsPerElement);
    }

    public class BloomFilterBuilder implements IIndexBulkLoader {
        private final long[] hashes = new long[2];
        private final long numElements;
        private final int numHashes;
        private final long numBits;
        private final int numPages;
        private IFIFOPageQueue queue;
        private ICachedPage[] pages;
        private ICachedPage metaDataPage = null;

        public BloomFilterBuilder(long numElements, int numHashes, int numBitsPerElement) throws HyracksDataException {
            if (!isActive) {
                throw new HyracksDataException("Failed to create the bloom filter builder since it is not activated.");
            }
            queue = bufferCache.createFIFOQueue();
            this.numElements = numElements;
            this.numHashes = numHashes;
            numBits = this.numElements * numBitsPerElement;
            long tmp = (long) Math.ceil(numBits / (double) numBitsPerPage);
            if (tmp > Integer.MAX_VALUE) {
                throw new HyracksDataException("Cannot create a bloom filter with his huge number of pages.");
            }
            numPages = (int) tmp;
            pages = new ICachedPage[numPages];
            int currentPageId = 1;
            while (currentPageId <= numPages) {
                ICachedPage page = bufferCache.confiscatePage(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
                initPage(page.getBuffer().array());
                pages[currentPageId - 1] = page;
                ++currentPageId;
            }
        }

        private void initPage(byte[] array) {
            int numRounds = array.length / ZERO_BUFFER.length;
            int leftOver = array.length % ZERO_BUFFER.length;
            int destPos = 0;
            for (int i = 0; i < numRounds; i++) {
                System.arraycopy(ZERO_BUFFER, 0, array, destPos, ZERO_BUFFER.length);
                destPos = (i + 1) * ZERO_BUFFER.length;
            }
            if (leftOver > 0) {
                System.arraycopy(ZERO_BUFFER, 0, array, destPos, leftOver);
            }
        }

        private void allocateAndInitMetaDataPage() throws HyracksDataException {
            if (metaDataPage == null) {
                metaDataPage = bufferCache.confiscatePage(BufferedFileHandle.getDiskPageId(fileId, METADATA_PAGE_ID));
            }
            metaDataPage.getBuffer().putInt(NUM_PAGES_OFFSET, numPages);
            metaDataPage.getBuffer().putInt(NUM_HASHES_USED_OFFSET, numHashes);
            metaDataPage.getBuffer().putLong(NUM_ELEMENTS_OFFSET, numElements);
            metaDataPage.getBuffer().putLong(NUM_BITS_OFFSET, numBits);
        }

        @Override
        public void add(ITupleReference tuple) throws IndexException, HyracksDataException {
            if (numPages == 0) {
                throw new HyracksDataException(
                        "Cannot add elements to this filter since it is supposed to be empty (number of elements hint passed to the filter during construction was 0).");
            }
            MurmurHash128Bit.hash3_x64_128(tuple, keyFields, SEED, hashes);
            for (int i = 0; i < numHashes; ++i) {
                long hash = Math.abs((hashes[0] + i * hashes[1]) % numBits);
                ICachedPage page = pages[((int) (hash / numBitsPerPage))];
                ByteBuffer buffer = page.getBuffer();
                int byteIndex = (int) (hash % numBitsPerPage) >> 3; // divide by 8
                byte b = buffer.get(byteIndex);
                int bitIndex = (int) (hash % numBitsPerPage) & 0x07; // mod 8
                b = (byte) (b | (1 << bitIndex));

                buffer.put(byteIndex, b);
            }
        }

        @Override
        public void end() throws HyracksDataException, IndexException {
            allocateAndInitMetaDataPage();
            queue.put(metaDataPage);
            for (ICachedPage p : pages) {
                queue.put(p);
            }
            bufferCache.finishQueue();
            BloomFilter.this.numBits = numBits;
            BloomFilter.this.numHashes = numHashes;
            BloomFilter.this.numElements = numElements;
            BloomFilter.this.numPages = numPages;
        }

        @Override
        public void abort() throws HyracksDataException {
            for (ICachedPage p : pages) {
                if (p != null) {
                    bufferCache.returnPage(p, false);
                }
            }
            if (metaDataPage != null) {
                bufferCache.returnPage(metaDataPage, false);
            }
        }

    }
}
