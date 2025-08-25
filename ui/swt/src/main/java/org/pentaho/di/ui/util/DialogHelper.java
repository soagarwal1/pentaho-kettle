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


package org.pentaho.di.ui.util;

import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.events.dialog.FilterType;
import org.pentaho.di.ui.core.events.dialog.ProviderFilterType;
import org.pentaho.di.ui.core.events.dialog.SelectionAdapterFileDialogTextVar;
import org.pentaho.di.ui.core.events.dialog.SelectionAdapterOptions;
import org.pentaho.di.ui.core.events.dialog.SelectionOperation;
import org.pentaho.di.ui.core.widget.TextVar;

/**
 * Created by bmorrise on 8/17/17.
 */
public class DialogHelper {
  public static SelectionAdapterFileDialogTextVar constructSelectionAdapterFileDialogTextVarForKettleFile( LogChannel log
                                                                                                           , TextVar wPath, AbstractMeta meta, SelectionOperation selectionOperation, FilterType filterType, Repository repository ) {
    if ( repository != null ) {
      ProviderFilterType[] providerFilterTypes = new ProviderFilterType[2];
      providerFilterTypes[0] = ProviderFilterType.RECENTS;
      providerFilterTypes[1] = ProviderFilterType.REPOSITORY;
      return new SelectionAdapterFileDialogTextVar( log, wPath, meta,
                                                    new SelectionAdapterOptions( meta.getBowl(), selectionOperation,
                                                                                 new FilterType[] { filterType,
        FilterType.XML, FilterType.ALL },
                                                                                 filterType, providerFilterTypes ) );
    }
    return new SelectionAdapterFileDialogTextVar( log, wPath, meta,
                                                  new SelectionAdapterOptions( meta.getBowl(), selectionOperation,
                                                                               new FilterType[] { filterType, FilterType.XML, FilterType.ALL }, filterType ) );
  }

  public static SelectionAdapterFileDialogTextVar constructSelectionAdapterFileDialogTextVarForUserFile( LogChannel log
                                                                                                         , TextVar wPath, AbstractMeta meta, SelectionOperation selectionOperation, FilterType[] filterTypes
                                                                                                         , FilterType defaultFilterType ) {
    ProviderFilterType[] providerFilterTypes = new ProviderFilterType[1];
    providerFilterTypes[0] = ProviderFilterType.DEFAULT;
    return new SelectionAdapterFileDialogTextVar( log, wPath, meta, new SelectionAdapterOptions( meta.getBowl(),
                                                                                                 selectionOperation, filterTypes, defaultFilterType, providerFilterTypes ) );
  }
}
