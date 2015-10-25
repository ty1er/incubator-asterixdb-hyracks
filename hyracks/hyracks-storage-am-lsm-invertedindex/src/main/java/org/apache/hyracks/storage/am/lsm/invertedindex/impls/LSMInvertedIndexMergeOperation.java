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

package org.apache.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMMergeOperation;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndex;

public class LSMInvertedIndexMergeOperation extends AbstractLSMMergeOperation {
    private final FileReference dictBTreeMergeTarget;
    private final FileReference deletedKeysBTreeMergeTarget;

    public LSMInvertedIndexMergeOperation(ILSMIndexAccessorInternal accessor, List<ILSMComponent> mergingComponents,
            ITreeIndexCursor cursor, FileReference dictBTreeMergeTarget, FileReference deletedKeysBTreeMergeTarget,
            FileReference bloomFilterMergeTarget, ILSMIOOperationCallback callback, String indexIdentifier) {
        super(accessor, mergingComponents, cursor, bloomFilterMergeTarget, callback, indexIdentifier);
        this.dictBTreeMergeTarget = dictBTreeMergeTarget;
        this.deletedKeysBTreeMergeTarget = deletedKeysBTreeMergeTarget;
    }

    @Override
    public Set<IODeviceHandle> getReadDevices() {
        Set<IODeviceHandle> devs = super.getReadDevices();
        for (Object o : mergingComponents) {
            LSMInvertedIndexDiskComponent component = (LSMInvertedIndexDiskComponent) o;
            OnDiskInvertedIndex invIndex = (OnDiskInvertedIndex) component.getInvIndex();
            devs.add(invIndex.getBTree().getFileReference().getDeviceHandle());
            devs.add(component.getDeletedKeysBTree().getFileReference().getDeviceHandle());
        }
        return devs;
    }

    @Override
    public Set<IODeviceHandle> getWriteDevices() {
        Set<IODeviceHandle> devs = super.getWriteDevices();
        devs.add(dictBTreeMergeTarget.getDeviceHandle());
        devs.add(deletedKeysBTreeMergeTarget.getDeviceHandle());
        return devs;
    }

    public FileReference getDictBTreeMergeTarget() {
        return dictBTreeMergeTarget;
    }

    public FileReference getDeletedKeysBTreeMergeTarget() {
        return deletedKeysBTreeMergeTarget;
    }

}