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


package org.pentaho.di.ui.core.widget;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;
import org.pentaho.di.core.variables.VariableSpace;

public   class InputTextVar extends Input<TextVar> {
  public InputTextVar( VariableSpace space, Composite composite, int width1, int width2 ) {
    super( space, composite, width1, width2 );
  }

  @Override
  protected void initText( VariableSpace space, Composite composite, int flags ) {
    input = new TextVar( space, this, SWT.LEFT | SWT.SINGLE | SWT.BORDER );
  }
}
