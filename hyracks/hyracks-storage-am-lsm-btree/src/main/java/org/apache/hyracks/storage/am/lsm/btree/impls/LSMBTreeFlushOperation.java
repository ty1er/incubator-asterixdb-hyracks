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

import java.util.Set;

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMFlushOperation;

public class LSMBTreeFlushOperation extends AbstractLSMFlushOperation implements Comparable<LSMBTreeFlushOperation> {

    private final FileReference btreeFlushTarget;
    private final FileReference statisticsFlushTarget;

    public LSMBTreeFlushOperation(ILSMIndexAccessorInternal accessor, ILSMComponent flushingComponent,
            FileReference btreeFlushTarget, FileReference bloomFilterFlushTarget, FileReference statisticsFlushTarget,
            ILSMIOOperationCallback callback, String indexIdentifier) {
        super(accessor, flushingComponent, bloomFilterFlushTarget, callback, indexIdentifier);
        this.btreeFlushTarget = btreeFlushTarget;
        this.statisticsFlushTarget = statisticsFlushTarget;
    }

    @Override
    public Set<IODeviceHandle> getWriteDevices() {
        Set<IODeviceHandle> devs = super.getWriteDevices();
        devs.add(btreeFlushTarget.getDeviceHandle());
        devs.add(statisticsFlushTarget.getDeviceHandle());
        return devs;
    }

    public FileReference getBTreeFlushTarget() {
        return btreeFlushTarget;
    }

    public FileReference getStatisticsTarget() {
        return statisticsFlushTarget;
    }

    @Override
    public int compareTo(LSMBTreeFlushOperation o) {
        return btreeFlushTarget.getFile().getName().compareTo(o.getBTreeFlushTarget().getFile().getName());
    }
}
