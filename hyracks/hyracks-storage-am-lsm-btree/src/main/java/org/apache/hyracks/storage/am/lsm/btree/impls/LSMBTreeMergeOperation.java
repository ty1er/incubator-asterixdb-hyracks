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

package org.apache.hyracks.storage.am.lsm.btree.impls;

import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMMergeOperation;

public class LSMBTreeMergeOperation extends AbstractLSMMergeOperation implements ILSMIOOperation {

    private final FileReference btreeMergeTarget;
    private final FileReference statisticsMergeTarget;

    public LSMBTreeMergeOperation(ILSMIndexAccessorInternal accessor, List<ILSMComponent> mergingComponents,
            ITreeIndexCursor cursor, FileReference btreeMergeTarget, FileReference bloomFilterMergeTarget,
            FileReference statisticsMergeTarget, ILSMIOOperationCallback callback, String indexIdentifier) {
        super(accessor, mergingComponents, cursor, bloomFilterMergeTarget, callback, indexIdentifier);
        this.btreeMergeTarget = btreeMergeTarget;
        this.statisticsMergeTarget = statisticsMergeTarget;
    }

    @Override
    public Set<IODeviceHandle> getReadDevices() {
        Set<IODeviceHandle> devs = super.getReadDevices();
        for (ILSMComponent o : mergingComponents) {
            LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) o;
            devs.add(component.getBTree().getFileReference().getDeviceHandle());
        }
        return devs;
    }

    @Override
    public Set<IODeviceHandle> getWriteDevices() {
        Set<IODeviceHandle> devs = super.getWriteDevices();
        devs.add(btreeMergeTarget.getDeviceHandle());
        devs.add(statisticsMergeTarget.getDeviceHandle());
        return devs;
    }

    public FileReference getBTreeMergeTarget() {
        return btreeMergeTarget;
    }

    public FileReference getStatisticsMergeTarget() {
        return statisticsMergeTarget;
    }
}
