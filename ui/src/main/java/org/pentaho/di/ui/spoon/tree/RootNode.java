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


package org.pentaho.di.ui.spoon.tree;

import org.eclipse.swt.graphics.Image;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.ui.core.widget.tree.TreeNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by bmorrise on 7/9/18.
 */
public class RootNode extends TreeNode {

  private List<TreeFolderProvider> treeFolderProviders = new ArrayList<>();
  private Map<AbstractMeta, TreeNode> abstractMetas = new ConcurrentHashMap<>();
  private Map<AbstractMeta, String> updates = new ConcurrentHashMap<>();

  public RootNode( String label, Image image, boolean expanded ) {
    super( label, image, expanded );
  }

  public void addProvider( TreeFolderProvider treeFolderProvider ) {
    treeFolderProviders.add( treeFolderProvider );
  }

  public void addProviders( List<TreeFolderProvider> treeFolderProviders ) {
    this.treeFolderProviders.addAll( treeFolderProviders );
  }

  public void addAbstractMeta( AbstractMeta abstractMeta, TreeNode treeNode ) {
    abstractMetas.put( abstractMeta, treeNode );
  }

  public TreeNode create( AbstractMeta abstractMeta, Image image, boolean expanded ) {
    TreeNode treeNode = new TreeNode( abstractMeta.getName(), image, expanded );
    addChild( treeNode );
    addAbstractMeta( abstractMeta, treeNode );
    treeFolderProviders.forEach( treeFolderProvider -> treeFolderProvider.create( abstractMeta, treeNode ) );
    return treeNode;
  }

  public void checkUpdate( AbstractMeta abstractMeta, String filter ) {
    TreeNode treeNode = abstractMetas.get( abstractMeta );
    if ( treeNode != null ) {
      for ( TreeFolderProvider treeFolderProvider : treeFolderProviders ) {
        TreeNode childTreeNode = getChildTreeNode( treeNode, treeFolderProvider.getTitle() );
        if ( childTreeNode != null ) {
          treeFolderProvider.checkUpdate( abstractMeta, childTreeNode, filter );
          if ( !Utils.isEmpty( filter ) ) {
            childTreeNode.setExpanded( true );
          }
        }
      }
    }
  }

  private TreeNode getChildTreeNode( TreeNode treeNode, String label ) {
    for ( TreeNode childTreeNode : treeNode.getChildren() ) {
      if ( childTreeNode.getLabel().equals( label ) ) {
        return childTreeNode;
      }
    }
    return null;
  }

  public String getNameByType( Class clazz ) {
    TreeFolderProvider treeFolderProvider = treeFolderProviders.stream()
            .filter( treeFolderProvider1 -> treeFolderProvider1.getType().equals( clazz ) )
            .findFirst()
            .orElse( null );
    if ( treeFolderProvider != null ) {
      return treeFolderProvider.getTitle();
    }
    return null;
  }

  public void remove( AbstractMeta abstractMeta ) {
    abstractMetas.remove( abstractMeta );
  }

  public TreeNode getTreeNode( AbstractMeta abstractMeta ) {
    return abstractMetas.get( abstractMeta );
  }

  public void update( String name ) {
    abstractMetas.keySet().forEach( abstractMeta -> updates.put( abstractMeta, name ) );
  }

  public void clearUpdates( AbstractMeta abstractMeta ) {
    updates.remove( abstractMeta );
  }

  public boolean hasNode( AbstractMeta abstractMeta ) {
    return abstractMetas.containsKey( abstractMeta );
  }

  public boolean shouldUpdate( AbstractMeta abstractMeta, String name ) {
    if ( updates.containsKey( abstractMeta ) && updates.get( abstractMeta ).equalsIgnoreCase( name ) ) {
      return true;
    }
    return false;
  }
}
