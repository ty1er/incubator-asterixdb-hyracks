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
package org.apache.hyracks.storage.am.lsm.rtree.impls;

import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMFlushOperation;

public class LSMRTreeFlushOperation extends AbstractLSMFlushOperation implements Comparable<LSMRTreeFlushOperation> {

    private final FileReference rtreeFlushTarget;
    private final FileReference btreeFlushTarget;

    public LSMRTreeFlushOperation(ILSMIndexAccessorInternal accessor, ILSMComponent flushingComponent,
            FileReference rtreeFlushTarget, FileReference btreeFlushTarget, FileReference bloomFilterFlushTarget,
            ILSMIOOperationCallback callback, String indexIdentifier) {
        super(accessor, flushingComponent, bloomFilterFlushTarget, callback, indexIdentifier);
        this.rtreeFlushTarget = rtreeFlushTarget;
        this.btreeFlushTarget = btreeFlushTarget;
    }

    @Override
    public Set<IODeviceHandle> getWriteDevices() {
        Set<IODeviceHandle> devs = new HashSet<IODeviceHandle>();
        devs.add(rtreeFlushTarget.getDeviceHandle());
        if (btreeFlushTarget != null) {
            devs.add(btreeFlushTarget.getDeviceHandle());
            devs.add(bloomFilterTarget.getDeviceHandle());
        }
        return devs;
    }

    public FileReference getRTreeFlushTarget() {
        return rtreeFlushTarget;
    }

    public FileReference getBTreeFlushTarget() {
        return btreeFlushTarget;
    }

    @Override
    public int compareTo(LSMRTreeFlushOperation o) {
        return rtreeFlushTarget.getFile().getName().compareTo(o.getRTreeFlushTarget().getFile().getName());
    }
}
