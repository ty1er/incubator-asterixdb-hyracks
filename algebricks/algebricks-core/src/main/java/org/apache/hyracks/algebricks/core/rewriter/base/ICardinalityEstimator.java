package org.apache.hyracks.algebricks.core.rewriter.base;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.storage.am.common.api.IIndexSearchArgument;

public interface ICardinalityEstimator {

    void getSelectivity(IIndexSearchArgument searchArg, IMetadataProvider metadataProvider, String dataverseName,
            String datasetName, String indexName) throws AlgebricksException;

}