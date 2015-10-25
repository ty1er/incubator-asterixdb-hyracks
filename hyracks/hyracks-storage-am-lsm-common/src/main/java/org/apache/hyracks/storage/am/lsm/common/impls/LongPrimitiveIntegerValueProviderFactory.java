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
package org.apache.hyracks.storage.am.lsm.common.impls;

import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.storage.am.common.api.IPrimitiveIntegerValueProvider;
import org.apache.hyracks.storage.am.common.api.IPrimitiveIntegerValueProviderFactory;

public class LongPrimitiveIntegerValueProviderFactory implements IPrimitiveIntegerValueProviderFactory {
    private static final long serialVersionUID = 1L;

    public static final LongPrimitiveIntegerValueProviderFactory INSTANCE = new LongPrimitiveIntegerValueProviderFactory();

    private LongPrimitiveIntegerValueProviderFactory() {
    }

    @Override
    public IPrimitiveIntegerValueProvider createPrimitiveIntegerValueProvider() {
        return new IPrimitiveIntegerValueProvider() {
            @Override
            public long getValue(byte[] bytes, int offset) {
                return LongPointable.getLong(bytes, offset);
            }

            @Override
            public long minDomainValue(byte[] bytes, int offset) {
                return Long.MIN_VALUE;
            }

            @Override
            public long maxDomainValue(byte[] bytes, int offset) {
                return Long.MAX_VALUE;
            }

            @Override
            public int maxLevel(byte[] bytes, int offset) {
                return Long.SIZE;
            }
        };
    }
}
