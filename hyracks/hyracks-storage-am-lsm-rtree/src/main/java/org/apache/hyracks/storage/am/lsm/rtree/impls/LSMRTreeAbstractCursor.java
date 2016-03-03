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

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ICursorInitialState;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.BloomFilterAwareBTreePointSearchCursor;
import org.apache.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import org.apache.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.am.rtree.impls.RTree.RTreeAccessor;
import org.apache.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public abstract class LSMRTreeAbstractCursor implements ITreeIndexCursor {

    protected boolean open;
    protected RTreeSearchCursor[] rtreeCursors;
    protected BTreeRangeSearchCursor[] btreeCursors;
    protected RTreeAccessor[] rtreeAccessors;
    protected BTreeAccessor[] btreeAccessors;
    private MultiComparator btreeCmp;
    protected int numberOfTrees;
    protected SearchPredicate rtreeSearchPredicate;
    protected RangePredicate btreeRangePredicate;
    protected ITupleReference frameTuple;
    protected boolean includeMutableComponent;
    protected ILSMHarness lsmHarness;
    protected boolean foundNext;
    protected final ILSMIndexOperationContext opCtx;

    protected List<ILSMComponent> operationalComponents;

    public LSMRTreeAbstractCursor(ILSMIndexOperationContext opCtx) {
        this.opCtx = opCtx;
        btreeRangePredicate = new RangePredicate(null, null, true, true, null, null);
    }

    public RTreeSearchCursor getCursor(int cursorIndex) {
        return rtreeCursors[cursorIndex];
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMRTreeCursorInitialState lsmInitialState = (LSMRTreeCursorInitialState) initialState;
        if (btreeCmp == null) {
            btreeCmp = lsmInitialState.getBTreeCmp();
            btreeRangePredicate.setLowKeyCmp(btreeCmp);
            btreeRangePredicate.setHighKeyCmp(btreeCmp);
        }
        operationalComponents = lsmInitialState.getOperationalComponents();
        lsmHarness = lsmInitialState.getLSMHarness();
        numberOfTrees = operationalComponents.size();

        int numComponenets = operationalComponents.size();
        if (rtreeCursors == null || rtreeCursors.length != numComponenets) {
            // object creation: should be relatively low
            rtreeCursors = new RTreeSearchCursor[numberOfTrees];
            btreeCursors = new BTreeRangeSearchCursor[numberOfTrees];
            rtreeAccessors = new RTreeAccessor[numberOfTrees];
            btreeAccessors = new BTreeAccessor[numberOfTrees];
        }

        includeMutableComponent = false;
        for (int i = 0; i < numberOfTrees; i++) {
            ILSMComponent component = operationalComponents.get(i);
            RTree rtree;
            BTree btree;
            if (component.getType() == LSMComponentType.MEMORY) {
                includeMutableComponent = true;
                // No need for a bloom filter for the in-memory BTree.
                if (btreeCursors[i] == null || btreeCursors[i].isBloomFilterAware()) {
                    //create
                    btreeCursors[i] = new BTreeRangeSearchCursor(
                            (IBTreeLeafFrame) lsmInitialState.getBTreeLeafFrameFactory().createFrame(), false);
                } else {
                    //re-use
                    btreeCursors[i].reset();
                }
                rtree = ((LSMRTreeMemoryComponent) component).getRTree();
                btree = ((LSMRTreeMemoryComponent) component).getBTree();
            } else {
                if (btreeCursors[i] == null || !btreeCursors[i].isBloomFilterAware()) {
                    // need to create a new one
                    btreeCursors[i] = new BloomFilterAwareBTreePointSearchCursor(
                            (IBTreeLeafFrame) lsmInitialState.getBTreeLeafFrameFactory().createFrame(), false,
                            ((LSMRTreeDiskComponent) operationalComponents.get(i)).getBloomFilter());
                } else {
                    // reset
                    ((BloomFilterAwareBTreePointSearchCursor) btreeCursors[i])
                            .resetBloomFilter(((LSMRTreeDiskComponent) operationalComponents.get(i)).getBloomFilter());
                    btreeCursors[i].reset();
                }
                rtree = ((LSMRTreeDiskComponent) component).getRTree();
                btree = ((LSMRTreeDiskComponent) component).getBTree();
            }
            if (rtreeCursors[i] == null) {
                rtreeCursors[i] = new RTreeSearchCursor(
                        (IRTreeInteriorFrame) lsmInitialState.getRTreeInteriorFrameFactory().createFrame(),
                        (IRTreeLeafFrame) lsmInitialState.getRTreeLeafFrameFactory().createFrame());
            } else {
                rtreeCursors[i].reset();
            }
            if (rtreeAccessors[i] == null) {
                rtreeAccessors[i] = (RTreeAccessor) rtree.createAccessor(NoOpOperationCallback.INSTANCE,
                        NoOpOperationCallback.INSTANCE);
                btreeAccessors[i] = (BTreeAccessor) btree.createAccessor(NoOpOperationCallback.INSTANCE,
                        NoOpOperationCallback.INSTANCE);
            } else {
                rtreeAccessors[i].reset(rtree, NoOpOperationCallback.INSTANCE);
                btreeAccessors[i].reset(btree, NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            }
        }

        rtreeSearchPredicate = (SearchPredicate) searchPred;
        btreeRangePredicate.setHighKey(null);
        btreeRangePredicate.setLowKey(null);

        open = true;
    }

    @Override
    public ICachedPage getPage() {
        // do nothing
        return null;
    }

    @Override
    public void close() throws HyracksDataException {
        if (!open) {
            return;
        }

        try {
            if (rtreeCursors != null && btreeCursors != null) {
                for (int i = 0; i < numberOfTrees; i++) {
                    rtreeCursors[i].close();
                    btreeCursors[i].close();
                }
            }
            rtreeCursors = null;
            btreeCursors = null;
        } finally {
            lsmHarness.endSearch(opCtx);
        }

        foundNext = false;
        open = false;
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        // do nothing
    }

    @Override
    public void setFileId(int fileId) {
        // do nothing
    }

    @Override
    public ITupleReference getTuple() {
        return frameTuple;
    }

    @Override
    public boolean exclusiveLatchNodes() {
        return false;
    }

    @Override
    public void markCurrentTupleAsUpdated() throws HyracksDataException {
        throw new HyracksDataException("Updating tuples is not supported with this cursor.");
    }
}
