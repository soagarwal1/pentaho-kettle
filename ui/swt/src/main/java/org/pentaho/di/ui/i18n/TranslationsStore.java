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


package org.pentaho.di.ui.i18n;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;

/**
 * This class contains and handles all the translations for the keys specified in the Java source code.
 *
 * @author matt
 *
 */
public class TranslationsStore {

  private List<String> localeList;

  /**
   * Locale - SourceStore
   */
  private Map<String, LocaleStore> localeMap;

  private String mainLocale;

  private Map<String, Map<String, List<KeyOccurrence>>> sourcePackageOccurrences;

  private LogChannelInterface log;

  /**
   * @param localeList
   * @param messagesPackages
   * @param map
   */
  public TranslationsStore( LogChannelInterface log, List<String> localeList, String mainLocale,
    Map<String, Map<String, List<KeyOccurrence>>> sourcePackageOccurrences ) {
    super();
    this.log = log;
    this.localeList = localeList;
    this.mainLocale = mainLocale;
    this.sourcePackageOccurrences = sourcePackageOccurrences;

    localeMap = new HashMap<String, LocaleStore>();
  }

  /**
   * Read all the translated messages for all the specified locale and all the specified locale
   *
   * @param directories
   *          The reference source directories to search packages in
   * @throws KettleException
   */
  public void read( List<String> directories ) throws KettleException {

    // The first locale (en_US) takes the lead: we need to find all of those
    // The others are optional.

    for ( String locale : localeList ) {
      LocaleStore localeStore = new LocaleStore( log, locale, mainLocale, sourcePackageOccurrences );
      try {
        localeStore.read( directories );
        localeMap.put( locale, localeStore );
      } catch ( Exception e ) {
        e.printStackTrace();
      }
    }
  }

  /**
   * @return the localeList
   */
  public List<String> getLocaleList() {
    return localeList;
  }

  /**
   * @param localeList
   *          the localeList to set
   */
  public void setLocaleList( List<String> localeList ) {
    this.localeList = localeList;
  }

  /**
   * @return the mainLocale
   */
  public String getMainLocale() {
    return mainLocale;
  }

  /**
   * @param mainLocale
   *          the mainLocale to set
   */
  public void setMainLocale( String mainLocale ) {
    this.mainLocale = mainLocale;
  }

  /**
   * Look up the translation for a key in a certain locale
   *
   * @param locale
   *          the locale to hunt for
   * @param sourceFolder
   *          the source folder to look in
   * @param messagesPackage
   *          the messages package to look in
   * @param key
   *          the key
   * @return the translation for the specified key in the desired locale, from the requested package
   */
  public String lookupKeyValue( String locale, String messagesPackage, String key ) {

    LocaleStore localeStore = localeMap.get( locale );
    if ( localeStore == null ) {
      return null;
    }

    for ( String sourceFolder : localeStore.getSourceMap().keySet() ) {
      SourceStore sourceStore = localeStore.getSourceMap().get( sourceFolder );

      MessagesStore messagesStore = sourceStore.getMessagesMap().get( messagesPackage );
      if ( messagesStore != null ) {
        String value = messagesStore.getMessagesMap().get( key );
        if ( value != null ) {
          return value;
        }
      }
    }

    return null;
  }

  public void removeValue( String locale, String sourceFolder, String messagesPackage, String key ) {
    LocaleStore localeStore = localeMap.get( locale );
    if ( localeStore == null ) {
      return;
    }

    SourceStore sourceStore = localeStore.getSourceMap().get( sourceFolder );
    if ( sourceStore == null ) {
      return;
    }

    MessagesStore messagesStore = sourceStore.getMessagesMap().get( messagesPackage );
    if ( messagesStore == null ) {
      return;
    }

    messagesStore.getMessagesMap().remove( key );
    messagesStore.setChanged();
  }

  public void storeValue( String locale, String sourceFolder, String messagesPackage, String key, String value ) {
    LocaleStore localeStore = localeMap.get( locale );
    if ( localeStore == null ) {
      localeStore = new LocaleStore( log, locale, mainLocale, sourcePackageOccurrences );
      localeMap.put( locale, localeStore );
    }

    SourceStore sourceStore = localeStore.getSourceMap().get( sourceFolder );
    if ( sourceStore == null ) {
      sourceStore = new SourceStore( log, locale, sourceFolder, sourcePackageOccurrences );
      localeStore.getSourceMap().put( sourceFolder, sourceStore );
    }

    MessagesStore messagesStore = sourceStore.getMessagesMap().get( messagesPackage );
    if ( messagesStore == null ) {
      messagesStore = new MessagesStore( locale, sourceFolder, messagesPackage, sourcePackageOccurrences );
      sourceStore.getMessagesMap().put( messagesPackage, messagesStore );
    }

    messagesStore.getMessagesMap().put( key, value );
    messagesStore.setChanged();
  }

  /**
   * @return the list of changed messages stores.
   */
  public List<MessagesStore> getChangedMessagesStores() {
    List<MessagesStore> list = new ArrayList<MessagesStore>();

    for ( LocaleStore localeStore : localeMap.values() ) {
      for ( SourceStore sourceStore : localeStore.getSourceMap().values() ) {
        for ( MessagesStore messagesStore : sourceStore.getMessagesMap().values() ) {
          if ( messagesStore.hasChanged() ) {
            list.add( messagesStore );
          }
        }
      }
    }

    return list;
  }

  /**
   * @param searchLocale
   *          the locale the filter on.
   * @param messagesPackage
   *          the messagesPackage to filter on. Specify null to get all message stores.
   * @return the list of messages stores for the main locale
   */
  public List<MessagesStore> getMessagesStores( String searchLocale, String messagesPackage ) {
    List<MessagesStore> list = new ArrayList<MessagesStore>();

    LocaleStore localeStore = localeMap.get( searchLocale );
    for ( SourceStore sourceStore : localeStore.getSourceMap().values() ) {
      for ( MessagesStore messagesStore : sourceStore.getMessagesMap().values() ) {
        if ( messagesPackage == null || messagesStore.getMessagesPackage().equals( messagesPackage ) ) {
          list.add( messagesStore );
        }
      }
    }

    return list;
  }

  public MessagesStore findMainLocaleMessagesStore( String sourceFolder, String messagesPackage ) {
    return localeMap.get( mainLocale ).getSourceMap().get( sourceFolder ).getMessagesMap().get( messagesPackage );
  }

  public Map<String, Map<String, List<KeyOccurrence>>> getSourcePackageOccurrences() {
    return sourcePackageOccurrences;
  }

  public void setSourcePackageOccurrences( Map<String, Map<String, List<KeyOccurrence>>> sourcePackageOccurrences ) {
    this.sourcePackageOccurrences = sourcePackageOccurrences;
  }
}
