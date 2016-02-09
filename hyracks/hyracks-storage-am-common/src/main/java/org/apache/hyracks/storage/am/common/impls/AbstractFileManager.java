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
package org.apache.hyracks.storage.am.common.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public abstract class AbstractFileManager {

    protected final IBufferCache bufferCache;
    protected final IFileMapProvider fileMapProvider;
    protected FileReference file;
    protected int fileId = -1;
    protected boolean isActive = false;

    public AbstractFileManager(IBufferCache bufferCache, IFileMapProvider fileMapProvider, FileReference file) {
        this.bufferCache = bufferCache;
        this.fileMapProvider = fileMapProvider;
        this.file = file;
    }

    public synchronized void create() throws HyracksDataException {
        if (isActive) {
            throw new HyracksDataException("Failed to create " + toString() + " since it is activated.");
        }

        prepareFile();
    }

    private void prepareFile() throws HyracksDataException {
        boolean fileIsMapped = false;
        synchronized (fileMapProvider) {
            fileIsMapped = fileMapProvider.isMapped(file);
            if (!fileIsMapped) {
                bufferCache.createFile(file);
            }
            fileId = fileMapProvider.lookupFileId(file);
            try {
                // Also creates the file if it doesn't exist yet.
                bufferCache.openFile(fileId);
            } catch (HyracksDataException e) {
                // Revert state of buffer cache since file failed to open.
                if (!fileIsMapped) {
                    bufferCache.deleteFile(fileId, false);
                }
                throw e;
            }
        }
    }

    public synchronized void activate() throws HyracksDataException {
        if (isActive) {
            throw new HyracksDataException("Failed to activate " + toString() + " since it is already activated.");
        }

        prepareFile();

        isActive = true;
    }

    public synchronized void deactivate() throws HyracksDataException {
        if (!isActive) {
            throw new HyracksDataException("Failed to deactivate " + toString() + " since it is already deactivated.");
        }

        bufferCache.closeFile(fileId);

        isActive = false;
    }

    public synchronized void destroy() throws HyracksDataException {
        if (isActive) {
            throw new HyracksDataException("Failed to destroy " + toString() + " since it is activated.");
        }

        if (fileId == -1) {
            return;
        }
        bufferCache.deleteFile(fileId, false);
        file.delete();
        fileId = -1;
    }

    public synchronized void clear() throws HyracksDataException {
        if (!isActive) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }
    }

    public int getFileId() {
        return fileId;
    }

    public FileReference getFileReference() {
        return file;
    }

    public IBufferCache getBufferCache() {
        return bufferCache;
    }

}