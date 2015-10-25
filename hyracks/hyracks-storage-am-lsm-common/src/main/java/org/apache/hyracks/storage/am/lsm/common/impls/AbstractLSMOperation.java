package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;

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