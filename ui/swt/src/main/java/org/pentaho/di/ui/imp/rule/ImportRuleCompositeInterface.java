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


package org.pentaho.di.ui.imp.rule;

import org.eclipse.swt.widgets.Composite;
import org.pentaho.di.imp.rule.ImportRuleInterface;

public interface ImportRuleCompositeInterface {

  /**
   * Get a composite with all controls on it that will allow a user to edit the import rule settings.
   *
   * @param parent
   *          The parent composite to place the new composite in.
   * @param importRule
   *          The import rule to edit
   * @return the new composite
   */
  public Composite getComposite( Composite parent, ImportRuleInterface importRule );

  /**
   * Set the import rule data onto the composite
   *
   * @param importRule
   *          the import rule to use
   */
  public void setCompositeData( ImportRuleInterface importRule );

  /**
   * Get the data from the composite controls and set them in the import rule provided.
   *
   * @param importRule
   *          The import rule to update.
   */
  public void getCompositeData( ImportRuleInterface importRule );
}
