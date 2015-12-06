package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
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

public abstract class AbstractLSMOperation implements ILSMIOOperation {

    protected final ILSMIndexAccessorInternal accessor;
    protected final FileReference bloomFilterTarget;
    protected final ILSMIOOperationCallback callback;
    protected final String indexIdentifier;

    public AbstractLSMOperation(ILSMIndexAccessorInternal accessor, FileReference bloomFilterTarget,
            ILSMIOOperationCallback callback, String indexIdentifier) {
        this.accessor = accessor;
        this.bloomFilterTarget = bloomFilterTarget;
        this.callback = callback;
        this.indexIdentifier = indexIdentifier;
    }

    @Override
    public ILSMIOOperationCallback getCallback() {
        return callback;
    }

    public FileReference getBloomFilterTarget() {
        return bloomFilterTarget;
    }

    public ILSMIndexAccessorInternal getAccessor() {
        return accessor;
    }

    @Override
    public String getIndexUniqueIdentifier() {
        return indexIdentifier;
    }

    @Override
    public Set<IODeviceHandle> getWriteDevices() {
        Set<IODeviceHandle> devs = new HashSet<IODeviceHandle>();
        devs.add(bloomFilterTarget.getDeviceHandle());
        return devs;
    }

}