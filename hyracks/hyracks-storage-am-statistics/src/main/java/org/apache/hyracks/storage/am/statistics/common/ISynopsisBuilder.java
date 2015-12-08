package org.apache.hyracks.storage.am.statistics.common;

import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;

public interface ISynopsisBuilder extends IIndexBulkLoader {

    public void setAntimatterTuple(boolean isAntimatter);

}
