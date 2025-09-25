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

import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.cluster.TransSplitter;

// TODO javadoc class and methods
public class ClusteredTransExecutorService implements TransExecutorService {
  private static Class<?> pkg = ClusteredTransExecutorService.class;

  TransSplitterExecutionService transSplitterExecutionService;
  private static final String DASHES = "-----------------------------------------------------";

  public ClusteredTransExecutorService() {
    this( new TransSplitterExecutionService() );
  }

  public ClusteredTransExecutorService( TransSplitterExecutionService transSplitterExecutionService )  {
    this.transSplitterExecutionService = transSplitterExecutionService;
  }

  /**
   * Execute transformation in clustered mode.
   */
  // POC NOTE: not sure if we need to pass the LogChannelInterface, but it's here as 'extLog' in the current code
  @Override
  public Result execute( LogChannelInterface extLog, TransMeta transMeta, Repository repository,
                         TransExecutionConfiguration executionConfiguration, String[] arguments ) throws KettleException {

    extLog.logBasic( BaseMessages.getString( pkg, "PanTransformationDelegate.Log.ExecutingClustered" ) );

    final TransSplitter transSplitter = new TransSplitter( transMeta );
    transSplitter.splitOriginalTransformation();

    Result result =  executeClustered( extLog, transMeta, transSplitter, executionConfiguration );
    logClusteredResults( extLog, transMeta, result );
    return result;
  }

  /**
   * Execute transformation in clustered mode.
   */ // FIXME FOCUS on testing this method
  protected Result executeClustered( LogChannelInterface extLog, TransMeta transMeta, TransSplitter transSplitter,
                                     TransExecutionConfiguration executionConfiguration ) throws KettleException {

    extLog.logBasic( BaseMessages.getString( pkg, "PanTransformationDelegate.Log.ExecutingClustered" ) );

    try {
      // Inject certain internal variables to make it more intuitive
      for ( String transVar : Const.INTERNAL_TRANS_VARIABLES ) {
        executionConfiguration.getVariables().put( transVar, transMeta.getVariable( transVar ) );
      }

      // Parameters override the variables
      TransMeta originalTransformation = transSplitter.getOriginalTransformation();
      for ( String param : originalTransformation.listParameters() ) {
        String value = Const.NVL( originalTransformation.getParameterValue( param ),
          Const.NVL( originalTransformation.getParameterDefault( param ),
            originalTransformation.getVariable( param ) ) );
        if ( !Utils.isEmpty( value ) ) {
          executionConfiguration.getVariables().put( param, value );
        }
      }

      // POC NOTE: all this logic was calls to static TransMethod with little interaction
      // NOW just test and verify behavior of transSplitterExecutionService#executeClustered
      return transSplitterExecutionService.executeClustered(
        extLog, transSplitter, null, executionConfiguration
      );

    } catch ( Exception e ) {
      throw new KettleException( e );
    }
  }

  /**
   * Log clustered transformation results.
   */
  protected void logClusteredResults( LogChannelInterface log, TransMeta transMeta, Result result ) {
    log.logBasic( DASHES );
    log.logBasic( "Got result back from clustered transformation:" );
    log.logBasic( transMeta + DASHES );
    log.logBasic( transMeta + " Errors : " + result.getNrErrors() );
    log.logBasic( transMeta + " Input : " + result.getNrLinesInput() );
    log.logBasic( transMeta + " Output : " + result.getNrLinesOutput() );
    log.logBasic( transMeta + " Updated : " + result.getNrLinesUpdated() );
    log.logBasic( transMeta + " Read : " + result.getNrLinesRead() );
    log.logBasic( transMeta + " Written : " + result.getNrLinesWritten() );
    log.logBasic( transMeta + " Rejected : " + result.getNrLinesRejected() );
    log.logBasic( transMeta + DASHES );
  }
}
