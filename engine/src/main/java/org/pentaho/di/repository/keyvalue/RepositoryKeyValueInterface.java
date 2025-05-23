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


package org.pentaho.di.repository.keyvalue;

import java.util.List;

import org.pentaho.di.core.exception.KettleException;

public interface RepositoryKeyValueInterface {

  public static final String NAMESPACE_VARIABLES = "Variables";
  public static final String NAMESPACE_DIMENSIONS = "Dimensions";

  /**
   * Store a value in the repository using the key/value interface
   *
   * @param namespace
   *          the name-space to reference
   * @param key
   *          The key to use
   * @param value
   *          The value to store
   * @throws KettleException
   *           in case there is an unexpected repository error
   */
  public void putValue( String namespace, String key, String value ) throws KettleException;

  /**
   * Remove a value from the repository key/value store
   *
   * @param namespace
   *          the name-space to reference
   * @param key
   *          The key of the value to remove
   * @throws KettleException
   *           in case there is an unexpected repository error
   */
  public void removeValue( String namespace, String key ) throws KettleException;

  /**
   * Load a value from the repository
   *
   * @param namespace
   *          The name-space to use
   * @param key
   *          The key to look up
   * @param revision
   *          The revision to use or null if you want the last revision (optionally supported)
   * @return The value including name, description, ...
   * @throws KettleException
   *           in case there is an unexpected repository error
   */
  public String getValue( String namespace, String key, String revision ) throws KettleException;

  /**
   * @return The list of name-spaces in the repository
   * @throws KettleException
   *           in case there is an unexpected repository error
   */
  public List<String> listNamespaces() throws KettleException;

  /**
   * List the keys for a given name-space in the repository
   *
   * @param namespace
   *          The name-space to query
   * @return The list of keys in the name-space
   * @throws KettleException
   *           in case there is an unexpected repository error
   */
  public List<String> listKeys( String namespace ) throws KettleException;

  /**
   * This method lists the key/value entries for a given name-space. Even though this method returns a
   * {@link RepositoryValueInterface} it does NOT (need to) load the actual object mentioned in it.
   *
   * @param namespace
   *          The name-space to query
   *
   * @return A list of value entries, unsorted.
   * @throws KettleException
   *           in case there is an unexpected repository error
   */
  public List<RepositoryValueInterface> listValues( String namespace ) throws KettleException;
}
