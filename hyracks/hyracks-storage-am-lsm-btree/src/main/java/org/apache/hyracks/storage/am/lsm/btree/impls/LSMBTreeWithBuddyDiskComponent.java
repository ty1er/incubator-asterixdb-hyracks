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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractDiskLSMComponent;

public class LSMBTreeWithBuddyDiskComponent extends AbstractDiskLSMComponent {

    private final BTree btree;
    private final BTree buddyBtree;

    public LSMBTreeWithBuddyDiskComponent(BTree btree, BTree buddyBtree, BloomFilter bloomFilter) {
        super(bloomFilter, null);
        this.btree = btree;
        this.buddyBtree = buddyBtree;
    }

    @Override
    protected void destroy() throws HyracksDataException {
        btree.deactivate();
        btree.destroy();
        buddyBtree.deactivate();
        buddyBtree.destroy();
        super.destroy();
    }

    public BTree getBTree() {
        return btree;
    }

    public BTree getBuddyBTree() {
        return buddyBtree;
    }

    @Override
    public long getComponentSize() {
        long size = super.getComponentSize();
        size = btree.getFileReference().getFile().length();
        size += buddyBtree.getFileReference().getFile().length();
        return size;
    }

    @Override
    public int getFileReferenceCount() {
        return btree.getBufferCache().getFileReferenceCount(btree.getFileId());
    }

}
