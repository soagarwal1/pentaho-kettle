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


package org.pentaho.di.ui.repository.repositoryexplorer.model;

import java.util.List;

import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.ui.xul.XulEventSourceAdapter;

public class UICluster extends XulEventSourceAdapter {

  private ClusterSchema cluster;

  public UICluster( ClusterSchema clusterSchema ) {
    this.cluster = clusterSchema;
  }

  public String getName() {
    if ( cluster != null ) {
      return cluster.getName();
    }
    return null;
  }

  public ClusterSchema getClusterSchema() {
    return this.cluster;
  }

  public String getServerList() {
    if ( cluster != null ) {
      List<SlaveServer> slaves = cluster.getSlaveServers();
      if ( slaves != null ) {
        StringBuilder sb = new StringBuilder();
        for ( SlaveServer slave : slaves ) {
          // Append separator before slave
          if ( sb.length() > 0 ) {
            sb.append( ", " );
          }
          sb.append( slave.getName() );
        }

        if ( sb.length() > 0 ) {
          return sb.toString();
        }
      }
    }
    return null;
  }

}
