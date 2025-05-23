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


package org.pentaho.di.ui.core;

import org.apache.commons.io.FilenameUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.util.Assert;
import org.pentaho.di.ui.core.widget.TextVar;

/**
 *
 * @author <a href="mailto:thomas.hoedl@aschauer-edv.at">Thomas Hoedl(asc042)</a>
 *
 */
public class SimpleFileSelection extends SelectionAdapter {

  /**
   * The default filter extension.
   */
  public static final String DEFAULT_FILTER_EXTENSION = "*";

  /**
   * The default file extension.
   */
  public static final String DEFAULT_FILTER_NAME = "All files (*.*)";

  private final Shell shell;

  private final TextVar textVar;

  private final String[] filterExtensions;

  private final String[] filterNames;

  /**
   * Constructor.
   *
   * @param shell
   *          shell to set.
   * @param textVar
   *          text variable to edit.
   * @param filterExtensions
   *          filter extensions to set.
   * @param filterNames
   *          filter names to set.
   * @throws IllegalArgumentException
   *           if shell or text variable is null.
   */
  public SimpleFileSelection( final Shell shell, final TextVar textVar, final String[] filterExtensions,
    final String[] filterNames ) throws IllegalArgumentException {
    super();
    Assert.assertNotNull( shell, "Shell cannot be null" );
    Assert.assertNotNull( textVar, "Text var cannot be null" );
    Assert.assertNotNull( filterNames, "Filter names cannot be null" );
    Assert.assertNotNull( filterExtensions, "Filter extensions cannot be null" );
    this.shell = shell;
    this.textVar = textVar;
    this.filterExtensions = new String[filterExtensions.length];
    System.arraycopy( filterExtensions, 0, this.filterExtensions, 0, filterExtensions.length );
    this.filterNames = new String[filterNames.length];
    System.arraycopy( filterNames, 0, this.filterNames, 0, filterNames.length );
  }

  /**
   * Constructor.
   *
   * @param shell
   *          the shell to set.
   * @param textVar
   *          the text variable to edit.
   * @throws IllegalArgumentException
   *           if shell or text variable is null.
   */
  public SimpleFileSelection( final Shell shell, final TextVar textVar ) throws IllegalArgumentException {
    this( shell, textVar, new String[] { DEFAULT_FILTER_EXTENSION }, new String[] { DEFAULT_FILTER_NAME } );
  }

  /**
   * Constructor.
   *
   * @param shell
   *          the shell to set.
   * @param textVar
   *          the text variable to edit.
   * @param filterNames
   *          the filter names to use.
   * @throws IllegalArgumentException
   *           if shell or text variable is null.
   */
  public SimpleFileSelection( final Shell shell, final TextVar textVar, final String... filterNames ) throws IllegalArgumentException {
    this( shell, textVar, new String[] { DEFAULT_FILTER_EXTENSION }, filterNames );
  }

  /**
   * {@inheritDoc}
   *
   * @see org.eclipse.swt.events.SelectionAdapter#widgetSelected(org.eclipse.swt.events.SelectionEvent)
   */
  @Override
  public void widgetSelected( final SelectionEvent event ) {
    final FileDialog dialog = new FileDialog( this.shell, SWT.OPEN );
    dialog.setFilterExtensions( this.filterExtensions );
    dialog.setFilterNames( this.filterNames );
    if ( this.textVar.getText() != null ) {
      dialog.setFileName( this.textVar.getText() );
    }
    if ( dialog.open() != null ) {
      final String filename = FilenameUtils.concat( dialog.getFilterPath(), dialog.getFileName() );
      this.textVar.setText( filename );
    }
  }

}
