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

package org.pentaho.di.ui.core.events.dialog;

/**
 * Interface used by ConditionSelectionAdapterFileDialogTextVar
 */
public interface DetermineSelectionOperationOp {
  /**
   * Operator that returns a SelectionOperation based on a condition.
   * @return SelectionOperation
   */
  SelectionOperation op();
}
