package org.apache.hyracks.storage.am.statistics.sketch;

public class GroupCountSketch extends Sketch {

    private final int levels;
    private final int depth;
    private final int buckets;
    private final int subbuckets;
    private final int fanoutLog;
    private final double counters[][][][];
    private final long[][] hashSeeds;

    public GroupCountSketch(int levels, int depth, int width, int fanoutLog/*double prob, double accuracy*/) {
        this.levels = levels;
        this.depth = depth;
        this.buckets = width;
        this.fanoutLog = fanoutLog;
        this.subbuckets = width * width;

        counters = new double[this.levels][this.depth][this.buckets][this.subbuckets];
        hashSeeds = new long[this.depth][8];
        initSeeds(this.depth, 8, hashSeeds);
    }

    public void update(long item, double diff) {
        int i, j, h, f, mult;
        long group;

        for (i = 0; i < depth; i++) {
            mult = HashGenerator.fourwise(this.hashSeeds[i][4], this.hashSeeds[i][5], this.hashSeeds[i][6],
                    this.hashSeeds[i][7], item);

            f = HashGenerator.hash31(this.hashSeeds[i][2], this.hashSeeds[i][3], item);
            f = f % (this.subbuckets);

            for (j = 0, group = item; j < levels; j++, group >>= fanoutLog) {

                h = HashGenerator.hash31(this.hashSeeds[i][0], this.hashSeeds[i][1], group);
                h = h % (this.buckets);

                if ((mult & 1) == 1)
                    this.counters[j][i][h][f] += diff;
                else
                    this.counters[j][i][h][f] -= diff;
            }
        }
    }

    public double count(int group, int level) {
        int h, f, mult;
        double[] estimates = new double[depth];

        for (int i = 0; i < depth; i++) {
            h = HashGenerator.hash31(this.hashSeeds[i][0], this.hashSeeds[i][1], group);
            h = h % (this.buckets);

            f = HashGenerator.hash31(this.hashSeeds[i][2], this.hashSeeds[i][3], group);
            f = f % (this.subbuckets);

            mult = HashGenerator.fourwise(this.hashSeeds[i][4], this.hashSeeds[i][5], this.hashSeeds[i][6],
                    this.hashSeeds[i][7], group);
            if ((mult & 1) == 1)
                estimates[i] += this.counters[level][i][h][f];
            else
                estimates[i] -= this.counters[level][i][h][f];
        }

        return getMedian(estimates, depth);
    }

    public double energyEst(int group, int level) {
        // estimate the F2 moment of the vector (sum of squares)

        int i, j;
        double z;

        double estimates[] = new double[depth];
        for (i = 0; i < depth; i++) {
            int h = HashGenerator.hash31(this.hashSeeds[i][0], this.hashSeeds[i][1], group);
            h = h % (this.buckets);
            z = 0;
            for (j = 0; j < this.subbuckets; j++) {
                z += Math.pow(this.counters[level][i][h][j], 2.0);
            }
            estimates[i] = z;
        }

        return getMedian(estimates, depth);
    }
}
