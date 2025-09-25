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

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.metastore.MetaStoreConst;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;

public class RemoteTransExecutorService implements TransExecutorService {

  private static Class<?> pkg = RemoteTransExecutorService.class;
  @Override
  public Result execute( LogChannelInterface log, TransMeta transMeta, Repository repository,
                         TransExecutionConfiguration executionConfiguration, String[] arguments ) throws KettleException {
    log.logBasic( BaseMessages.getString( pkg, "PanTransformationDelegate.Log.ExecutingRemotely" ) );

    if ( executionConfiguration.getRemoteServer() == null ) {
      throw new KettleException( BaseMessages.getString( pkg, "PanTransformationDelegate.Error.NoRemoteServerSpecified" ) );
    }

    // Send transformation to slave server
    String carteObjectId = Trans.sendToSlaveServer( transMeta, executionConfiguration, repository, MetaStoreConst.getDefaultMetastore() );

    // Monitor remote transformation
    monitorRemoteTransformation( log, transMeta, carteObjectId, executionConfiguration.getRemoteServer() );

    // For command-line execution, we typically return a simple success result
    // In a real implementation, you might want to fetch the actual result from the remote server
    Result result = new Result();
    result.setResult( true );
    return result;
  }

  /**
   * Monitor remote transformation execution.
   */
  protected void monitorRemoteTransformation( LogChannelInterface log,
                                              final TransMeta transMeta,
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
}
