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

public class HashGenerator {

    private static int MOD = 2147483647;
    private static int HL = 31;

    public static int hash31(long a, long b, long x) {

        long result;

        // return a hash of x using a and b mod (2^31 - 1)
        // may need to do another mod afterwards, or drop high bits
        // depending on d, number of bad guys
        // 2^31 - 1 = 2147483647

        //  result = ((long long) a)*((long long) x)+((long long) b);
        result = (a * x) + b;
        result = ((result >> HL) + result) & MOD;

        return (int) result;
    }

    public static int fourwise(long a, long b, long c, long d, long x) {
        int result;

        // returns values that are 4-wise independent by repeated calls
        // to the pairwise independent routine.

        result = hash31(hash31(hash31(x, a, b), x, c), x, d);
        return result;
    }
}