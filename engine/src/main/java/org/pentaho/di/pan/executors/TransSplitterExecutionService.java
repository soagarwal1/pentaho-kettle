/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.di.pan.executors;

import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.job.Job;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.cluster.TransSplitter;

public class TransSplitterExecutionService {

  public Result executeClustered( LogChannelInterface extLog, TransSplitter transSplitter, Job parentJob,
                                  TransExecutionConfiguration executionConfiguration ) throws KettleException {
    executeClustered( extLog, transSplitter, executionConfiguration );
    // Monitor clustered transformation
    Trans.monitorClusteredTransformation( extLog, transSplitter, parentJob ); // TODO should be able to test using Mockitos MockedStatic
    return Trans.getClusteredTransformationResult( extLog, transSplitter, parentJob );
  }

  protected void executeClustered( LogChannelInterface extLog, TransSplitter transSplitter, TransExecutionConfiguration executionConfiguration )
    throws KettleException {
    // Execute clustered transformation
    try {
      Trans.executeClustered( transSplitter, executionConfiguration );
    } catch ( Exception e ) {
      cleanupClusterAfterError( extLog, transSplitter, e );
    }
  }

  protected void cleanupClusterAfterError( LogChannelInterface extLog, TransSplitter transSplitter, Exception e ) throws KettleException {
    // Clean up cluster in case of error
    try {
      Trans.cleanupCluster( extLog, transSplitter );
    } catch ( Exception cleanupException ) {
      throw new KettleException( "Error executing transformation and error cleaning up cluster", e );
    }
  }
}
