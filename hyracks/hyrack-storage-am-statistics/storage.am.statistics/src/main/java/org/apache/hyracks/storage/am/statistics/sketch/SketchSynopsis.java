package org.apache.hyracks.storage.am.statistics.sketch;

//public class SketchSynopsis extends Synopsis implements IIndexBulkLoader {
//
//    private final int levelNum;
//    private final int fanoutLog;
//    private final double epsilon;
//    private final GroupCountSketch gcSketch;
//    private final IBufferCache bufferCache;
//    private final IFileMapProvider fileMapProvider;
//    private final FileReference file;
//    private final int[] keyFields;
//    private final int fileId = -1;
//    private final boolean isActivated = false;
//
//    public SketchSynopsis(IBufferCache bufferCache, IFileMapProvider fileMapProvider, FileReference file,
//            int[] keyFields, int domainSize, double delta, double epsilon, int fanOut) {
//        super(bufferCache, fileMapProvider, file);
//        this.keyFields = keyFields;
//        this.fanoutLog = (int) (Math.log(fanOut) / Math.log(2.0));
//        this.levelNum = domainSize / fanoutLog;
//        this.epsilon = epsilon;
//        final int depth = (int) Math.ceil(Math.log(1 / delta));
//        final int width = (int) Math.ceil(1 / epsilon);
//        gcSketch = new GroupCountSketch(this.levelNum + 1, depth, width, fanoutLog);
//    }
//
//    public void update(long item, double diff) {
//        //translate position to coefficient
//        item += 1 << (levelNum * fanoutLog);
//        //transform update into wavelet domain
//        long div = 1;
//        for (int i = 0; i < levelNum; i++) {
//            //            Long coeffIdx = (long) ((1 << ((levelNum - i) * fanoutLog)) + item);
//            item >>= (fanoutLog - 1);
//            int sign = (item & 1) == 0 ? 1 : -1;
//            item >>= 1;
//            double normCoeff = WaveletCoefficient.getNormalizationCoefficient(levelNum * fanoutLog,
//                    (i + 1) * fanoutLog);
//            div = (1 << ((i + 1) * fanoutLog));
//
//            gcSketch.update(item, diff * sign / (normCoeff * div));
//        }
//        gcSketch.update(0, diff / div);
//    }
//
//    @Override
//    public void add(ITupleReference tuple) throws IndexException, HyracksDataException {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public void end() throws IndexException, HyracksDataException {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public void create() {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public void activate() {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public IIndexBulkLoader createBuilder(long numElements) {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public void addElement(Object key, Object value) {
//        // TODO Auto-generated method stub
//
//    }
//
//}
