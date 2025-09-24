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

package org.pentaho.di.pan.delegates;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPointHandler;
import org.pentaho.di.core.extension.KettleExtensionPoint;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.metastore.MetaStoreConst;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.DefaultTransFactoryManager;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.cluster.TransSplitter;

import java.util.HashMap;
import java.util.Map;

/**
 * Delegate class for handling transformation execution in command-line contexts (Pan).
 * This class centralizes the execution logic similar to SpoonTransformationDelegate
 * but is designed for non-UI execution environments.
 */
public class PanTransformationDelegate {

  private static Class<?> pkg = PanTransformationDelegate.class;

  private LogChannelInterface log;
  private Repository repository;

  private static final String DASHES = "-----------------------------------------------------";

  public PanTransformationDelegate( LogChannelInterface log ) {
    this.log = log;
  }

  public PanTransformationDelegate( LogChannelInterface log, Repository repository ) {
    this.log = log;
    this.repository = repository;
  }

  /**
   * Execute a transformation with the specified configuration.
   *
   * @param trans              the transformation
   * @param executionConfiguration the execution configuration
   * @param arguments              command line arguments
   * @return the execution result
   * @throws KettleException if execution fails
   */
  public Result executeTransformation( final Trans trans,
                                       final TransExecutionConfiguration executionConfiguration,
                                       final String[] arguments ) throws KettleException {

    TransMeta transMeta = trans.getTransMeta();
    if ( transMeta == null ) {
      throw new KettleException( BaseMessages.getString( pkg, "PanTransformationDelegate.Error.TransMetaNull" ) );
    }

    // Set repository and metastore information in both the exec config and the metadata
    transMeta.setRepository( repository );
    transMeta.setMetaStore( MetaStoreConst.getDefaultMetastore() );
    executionConfiguration.setRepository( repository );

    // Set the run options
    transMeta.setClearingLog( executionConfiguration.isClearingLog() );
    transMeta.setSafeModeEnabled( executionConfiguration.isSafeModeEnabled() );
    transMeta.setGatheringMetrics( executionConfiguration.isGatheringMetrics() );

    // Call extension points
    ExtensionPointHandler.callExtensionPoint( log, KettleExtensionPoint.SpoonTransMetaExecutionStart.id, transMeta );
    ExtensionPointHandler.callExtensionPoint( log, KettleExtensionPoint.SpoonTransExecutionConfiguration.id, executionConfiguration );

    try {
      ExtensionPointHandler.callExtensionPoint( log, KettleExtensionPoint.SpoonTransBeforeStart.id, new Object[] {
        executionConfiguration, transMeta, transMeta, repository
      } );
    } catch ( KettleException e ) {
      log.logError( e.getMessage(), transMeta.getFilename() );
      throw e;
    }

    // Apply parameters
    Map<String, String> paramMap = executionConfiguration.getParams();
    for ( Map.Entry<String, String> entry : paramMap.entrySet() ) {
      transMeta.setParameterValue( entry.getKey(), Const.NVL( entry.getValue(), "" ) );
    }
    transMeta.activateParameters();

    // Set the log level
    if ( executionConfiguration.getLogLevel() != null ) {
      transMeta.setLogLevel( executionConfiguration.getLogLevel() );
    }

    // Determine execution type and execute accordingly
    return executeBasedOnConfiguration( transMeta, executionConfiguration, arguments );
  }

  /**
   * Execute transformation based on the execution configuration type.
   */
  private Result executeBasedOnConfiguration( TransMeta transMeta,
                                              TransExecutionConfiguration executionConfiguration,
                                              String[] arguments ) throws KettleException {

    // Is this a local execution?
    if ( executionConfiguration.isExecutingLocally() ) {
      return executeLocally( transMeta, executionConfiguration, arguments );

    } else if ( executionConfiguration.isExecutingRemotely() ) {
      return executeRemotely( transMeta, executionConfiguration );

    } else if ( executionConfiguration.isExecutingClustered() ) {
      return executeClustered( transMeta, executionConfiguration );

    } else {
      throw new KettleException( BaseMessages.getString( pkg, "PanTransformationDelegate.Error.NoExecutionTypeSpecified" ) );
    }
  }

  /**
   * Execute transformation locally.
   */
  private Result executeLocally( TransMeta transMeta,
                                 TransExecutionConfiguration executionConfiguration,
                                 String[] arguments ) throws KettleException {

    log.logBasic( BaseMessages.getString( pkg, "PanTransformationDelegate.Log.ExecutingLocally" ) );

    Trans trans = DefaultTransFactoryManager.getInstance().getTransFactory( executionConfiguration.getRunConfiguration() ).create( transMeta, transMeta );
    trans.setRepository( repository );
    trans.setMetaStore( MetaStoreConst.getDefaultMetastore() );

    // Copy execution configuration settings
    trans.setLogLevel( executionConfiguration.getLogLevel() );
    trans.setSafeModeEnabled( executionConfiguration.isSafeModeEnabled() );
    trans.setGatheringMetrics( executionConfiguration.isGatheringMetrics() );

    // Apply variables from execution configuration
    Map<String, String> variables = executionConfiguration.getVariables();
    for ( Map.Entry<String, String> entry : variables.entrySet() ) {
      trans.setVariable( entry.getKey(), entry.getValue() );
    }

    // Prepare and start execution
    trans.prepareExecution( arguments );
    trans.startThreads();

    // Wait for completion
    trans.waitUntilFinished();

    return trans.getResult();
  }

  /**
   * Execute transformation remotely.
   */
  private Result executeRemotely( TransMeta transMeta,
                                  TransExecutionConfiguration executionConfiguration ) throws KettleException {

    log.logBasic( BaseMessages.getString( pkg, "PanTransformationDelegate.Log.ExecutingRemotely" ) );

    if ( executionConfiguration.getRemoteServer() == null ) {
      throw new KettleException( BaseMessages.getString( pkg, "PanTransformationDelegate.Error.NoRemoteServerSpecified" ) );
    }

    // Send transformation to slave server
    String carteObjectId = Trans.sendToSlaveServer( transMeta, executionConfiguration, repository, MetaStoreConst.getDefaultMetastore() );

    // Monitor remote transformation
    monitorRemoteTransformation( transMeta, carteObjectId, executionConfiguration.getRemoteServer() );

    // For command-line execution, we typically return a simple success result
    // In a real implementation, you might want to fetch the actual result from the remote server
    Result result = new Result();
    result.setResult( true );
    return result;
  }

  /**
   * Execute transformation in clustered mode.
   */
  protected Result executeClustered( TransMeta transMeta,
                                   TransExecutionConfiguration executionConfiguration ) throws KettleException {

    log.logBasic( BaseMessages.getString( pkg, "PanTransformationDelegate.Log.ExecutingClustered" ) );

    try {
      final TransSplitter transSplitter = new TransSplitter( transMeta );
      transSplitter.splitOriginalTransformation();

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
      executeClustered( transSplitter, executionConfiguration );
      // Monitor clustered transformation
      Trans.monitorClusteredTransformation( log, transSplitter, null );
      Result result = Trans.getClusteredTransformationResult( log, transSplitter, null );

      logClusteredResults( transMeta, result );

      return result;

    } catch ( Exception e ) {
      throw new KettleException( e );
    }
  }

  public void executeClustered( TransSplitter transSplitter, TransExecutionConfiguration executionConfiguration )
    throws KettleException {
    // Execute clustered transformation
    try {
      Trans.executeClustered( transSplitter, executionConfiguration );
    } catch ( Exception e ) {
      cleanupClusterAfterError( transSplitter, e );
    }
  }
  public void cleanupClusterAfterError( TransSplitter transSplitter, Exception e ) throws KettleException {
    // Clean up cluster in case of error
    try {
      Trans.cleanupCluster( log, transSplitter );
    } catch ( Exception cleanupException ) {
      throw new KettleException( "Error executing transformation and error cleaning up cluster", e );
    }
  }

  /**
   * Monitor remote transformation execution.
   */
  protected void monitorRemoteTransformation( final TransMeta transMeta,
                                            final String carteObjectId,
                                            final SlaveServer remoteSlaveServer ) {

    // Launch in a separate thread to prevent blocking
    Thread monitorThread = new Thread( () ->
      Trans.monitorRemoteTransformation( log, carteObjectId, transMeta.toString(), remoteSlaveServer )
    );

    monitorThread.setName( "Monitor remote transformation '" + transMeta.getName()
      + "', carte object id=" + carteObjectId
      + ", slave server: " + remoteSlaveServer.getName() );
    monitorThread.start();

    // For command-line execution, we might want to wait for completion
    try {
      monitorThread.join();
    } catch ( InterruptedException e ) {
      log.logError( "Interrupted while monitoring remote transformation", e );
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Log clustered transformation results.
   */
  protected void logClusteredResults( TransMeta transMeta, Result result ) {
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

  /**
   * Create a default execution configuration for command-line execution.
   */
  public static TransExecutionConfiguration createDefaultExecutionConfiguration() {
    TransExecutionConfiguration config = new TransExecutionConfiguration();

    // Set defaults for command-line execution
    config.setExecutingLocally( true );
    config.setExecutingRemotely( false );
    config.setExecutingClustered( false );
    config.setClearingLog( true );
    config.setSafeModeEnabled( false );
    config.setGatheringMetrics( false );
    config.setLogLevel( LogLevel.BASIC );

    // Initialize empty collections
    config.setVariables( new HashMap<>() );
    config.setParams( new HashMap<>() );

    return config;
  }

  /**
   * Create an execution configuration for remote execution.
   */
  public static TransExecutionConfiguration createRemoteExecutionConfiguration( SlaveServer slaveServer ) {
    TransExecutionConfiguration config = createDefaultExecutionConfiguration();

    config.setExecutingLocally( false );
    config.setExecutingRemotely( true );
    config.setRemoteServer( slaveServer );

    return config;
  }

  /**
   * Create an execution configuration for clustered execution.
   */
  public static TransExecutionConfiguration createClusteredExecutionConfiguration() {
    TransExecutionConfiguration config = createDefaultExecutionConfiguration();

    config.setExecutingLocally( false );
    config.setExecutingClustered( true );
    config.setClusterPosting( true );
    config.setClusterPreparing( true );
    config.setClusterStarting( true );
    config.setClusterShowingTransformation( false );

    return config;
  }

  // Getters and setters
  public LogChannelInterface getLog() {
    return log;
  }

  public void setLog( LogChannelInterface log ) {
    this.log = log;
  }

  public Repository getRepository() {
    return repository;
  }

  public void setRepository( Repository repository ) {
    this.repository = repository;
  }
}
