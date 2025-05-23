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


package org.pentaho.di.ui.core.database.dialog.tags;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.Composite;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.containers.XulTree;
import org.pentaho.ui.xul.dom.Element;
import org.pentaho.ui.xul.jface.tags.JfaceCMenuList;

/*
 * a Menu List with variable substitution capability
 */
public class ExtMenuList extends JfaceCMenuList {
  public ComboVar extCombo;
  private VariableSpace variableSpace;
  private XulComponent xulParent;

  private int style = SWT.NONE;

  public ExtMenuList( Element self, XulComponent parent, XulDomContainer container, String tagName ) {
    super( self, parent, container, tagName );
    xulParent = parent;
    createNewExtMenuList( parent );
  }

  @Override
  protected void setupCombobox() {

  }

  private void createNewExtMenuList( XulComponent parent ) {
    xulParent = parent;

    if ( ( xulParent != null ) && ( xulParent instanceof XulTree ) ) {
      variableSpace = (DatabaseMeta) ( (XulTree) xulParent ).getData();

    } else {
      variableSpace = new DatabaseMeta();
      style = SWT.BORDER;
    }

    extCombo = new ComboVar( variableSpace, (Composite) parent.getManagedObject(), style );
    combobox = extCombo.getCComboWidget();
    setManagedObject( extCombo );

    combobox.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        fireSelectedEvents();
      }
    } );

    combobox.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent modifyEvent ) {
        fireModifiedEvents();
      }
    } );
  }

  public void setVariableSpace( VariableSpace space ) {
    variableSpace = space;
    extCombo.setVariables( variableSpace );
  }

}
