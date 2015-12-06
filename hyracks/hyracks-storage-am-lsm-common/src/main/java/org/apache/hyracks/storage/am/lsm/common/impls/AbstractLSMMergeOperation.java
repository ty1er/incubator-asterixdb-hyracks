package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;

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

public abstract class AbstractLSMMergeOperation extends AbstractLSMOperation {

    protected final List<ILSMComponent> mergingComponents;
    protected final ITreeIndexCursor cursor;

    public AbstractLSMMergeOperation(ILSMIndexAccessorInternal accessor, List<ILSMComponent> mergingComponents,
            ITreeIndexCursor cursor, FileReference bloomFilterMergeTarget, ILSMIOOperationCallback callback,
            String indexIdentifier) {
        super(accessor, bloomFilterMergeTarget, callback, indexIdentifier);
        this.cursor = cursor;
        this.mergingComponents = mergingComponents;
    }

    @Override
    public Set<IODeviceHandle> getReadDevices() {
        Set<IODeviceHandle> devs = new HashSet<IODeviceHandle>();
        for (ILSMComponent o : mergingComponents) {
            AbstractDiskLSMComponent component = (AbstractDiskLSMComponent) o;
            devs.add(component.getBloomFilter().getFileReference().getDeviceHandle());
        }
        return devs;
    }

    @Override
    public Boolean call() throws HyracksDataException, IndexException {
        accessor.merge(this);
        return true;
    }

    public ITreeIndexCursor getCursor() {
        return cursor;
    }

    public List<ILSMComponent> getMergingComponents() {
        return mergingComponents;
    }

    @Override
    public LSMIOOpertionType getIOOpertionType() {
        return LSMIOOpertionType.MERGE;
    }

}