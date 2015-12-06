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

package org.apache.hyracks.storage.am.statistics.sketch;

import java.util.Random;

public abstract class Sketch {

    protected void initSeeds(int k, int m, long[][] hashSeeds) {
        Random prng = new Random();

        int j, i;
        for (i = 0; i < k; i++) {
            for (j = 0; j < m; j++) {
                hashSeeds[i][j] = Math.abs(prng.nextLong()); //(int) prng.genInt();
                // initialise the hash functions
                // prng_int() should return a random integer
                // uniformly distributed in the range 0..2^31
            }
        }
    }

    protected static double getMedian(double[] data, int length) {
        return QuickSelect.select(data, data.length / 2);
    }
}
