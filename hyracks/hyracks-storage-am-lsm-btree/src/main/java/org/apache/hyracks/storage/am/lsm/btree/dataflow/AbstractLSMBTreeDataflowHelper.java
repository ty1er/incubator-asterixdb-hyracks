package org.apache.hyracks.storage.am.lsm.btree.dataflow;

import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.IOrdinalPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.dataflow.AbstractLSMIndexDataflowHelper;

public abstract class AbstractLSMBTreeDataflowHelper extends AbstractLSMIndexDataflowHelper {

    protected final boolean collectStatistics;
    protected final IOrdinalPrimitiveValueProviderFactory statsFieldValueProviderFactory;
    protected final ITypeTraits[] statsFieldTypeTraits;

    public AbstractLSMBTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            List<IVirtualBufferCache> virtualBufferCaches, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTrackerProvider opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            boolean durable, boolean collectStatistics, ITypeTraits[] statsFieldTypeTraits,
            IOrdinalPrimitiveValueProviderFactory statsFieldValueProviderFactory) {
        super(opDesc, ctx, partition, virtualBufferCaches, bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory,
                ioScheduler, ioOpCallbackFactory, filterTypeTraits, filterCmpFactories, filterFields, durable);
        this.collectStatistics = collectStatistics;
        this.statsFieldValueProviderFactory = statsFieldValueProviderFactory;
        this.statsFieldTypeTraits = statsFieldTypeTraits;
    }
}
