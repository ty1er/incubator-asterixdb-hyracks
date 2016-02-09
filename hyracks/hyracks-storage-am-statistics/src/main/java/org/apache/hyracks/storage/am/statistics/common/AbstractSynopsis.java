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
package org.apache.hyracks.storage.am.statistics.common;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ISynopsis;
import org.apache.hyracks.storage.am.common.api.ISynopsisElement;

public abstract class AbstractSynopsis<T extends ISynopsisElement> implements ISynopsis<T> {

    protected static final long serialVersionUID = 1L;

    protected final long domainEnd;
    protected final long domainStart;
    protected final int maxLevel;
    protected final int size;
    protected final ITypeTraits keyTypeTraits;

    public AbstractSynopsis(ITypeTraits keyTypeTraits, int size) throws HyracksDataException {
        this.keyTypeTraits = keyTypeTraits;
        this.domainStart = TypeTraitsDomainUtils.minDomainValue(keyTypeTraits);
        this.domainEnd = TypeTraitsDomainUtils.maxDomainValue(keyTypeTraits);
        this.maxLevel = TypeTraitsDomainUtils.maxLevel(keyTypeTraits);
        this.size = size;
    }

    public long getDomainEnd() {
        return domainEnd;
    }

    public long getDomainStart() {
        return domainStart;
    }

    public int getMaxLevel() {
        return maxLevel;
    }

    @Override
    public ITypeTraits getKeyTypeTraits() {
        return keyTypeTraits;
    }

    @Override
    public int size() {
        return size;
    }

}
