package org.apache.hyracks.storage.am.common.statistics.sketch;

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
