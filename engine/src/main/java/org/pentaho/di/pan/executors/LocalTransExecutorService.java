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
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.metastore.MetaStoreConst;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.DefaultTransFactoryManager;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;

import java.util.Map;

public class LocalTransExecutorService implements TransExecutorService {

  private static Class<?> pkg = LocalTransExecutorService.class;

  @Override
  public Result execute( LogChannelInterface log, TransMeta transMeta, Repository repository,
                         TransExecutionConfiguration executionConfiguration, String[] arguments ) throws KettleException {
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
}
