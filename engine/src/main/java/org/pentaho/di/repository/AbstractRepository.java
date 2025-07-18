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


package org.pentaho.di.repository;

import org.pentaho.di.core.bowl.Bowl;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.ProgressMonitorListener;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;

import java.util.List;

/**
 * Implementing convenience methods that can be described in terms of other methods in the interface
 */
public abstract class AbstractRepository implements Repository {

  private final Bowl bowl = new RepositoryBowl( this );

  @Override
  public long getJobEntryAttributeInteger( ObjectId id_jobentry, String code ) throws KettleException {
    return getJobEntryAttributeInteger( id_jobentry, 0, code );
  }

  @Override
  public String getJobEntryAttributeString( ObjectId id_jobentry, String code ) throws KettleException {
    return getJobEntryAttributeString( id_jobentry, 0, code );
  }

  @Override
  public boolean getJobEntryAttributeBoolean( ObjectId id_jobentry, String code ) throws KettleException {
    return getJobEntryAttributeBoolean( id_jobentry, 0, code );
  }

  @Override
  public boolean getJobEntryAttributeBoolean( ObjectId id_jobentry, String code, boolean def ) throws KettleException {
    return getJobEntryAttributeBoolean( id_jobentry, 0, code, def );
  }

  @Override
  public boolean getJobEntryAttributeBoolean( ObjectId id_jobentry, int nr, String code ) throws KettleException {
    return getJobEntryAttributeBoolean( id_jobentry, nr, code, false );
  }

  public abstract boolean getJobEntryAttributeBoolean( ObjectId id_jobentry, int nr, String code, boolean def ) throws KettleException;

  @Override
  public boolean getStepAttributeBoolean( ObjectId id_step, String code ) throws KettleException {
    return getStepAttributeBoolean( id_step, 0, code );
  }

  @Override
  public boolean getStepAttributeBoolean( ObjectId id_step, int nr, String code ) throws KettleException {
    return getStepAttributeBoolean( id_step, nr, code, false );
  }

  @Override
  public long getStepAttributeInteger( ObjectId id_step, String code ) throws KettleException {
    return getStepAttributeInteger( id_step, 0, code );
  }

  @Override
  public String getStepAttributeString( ObjectId id_step, String code ) throws KettleException {
    return getStepAttributeString( id_step, 0, code );
  }

  @Override
  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, String code, boolean value ) throws KettleException {
    saveStepAttribute( id_transformation, id_step, 0, code, value );
  }

  @Override
  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, String code, double value ) throws KettleException {
    saveStepAttribute( id_transformation, id_step, 0, code, value );
  }

  @Override
  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, String code, long value ) throws KettleException {
    saveStepAttribute( id_transformation, id_step, 0, code, value );
  }

  @Override
  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, String code, String value ) throws KettleException {
    saveStepAttribute( id_transformation, id_step, 0, code, value );
  }

  @Override
  public void saveJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, String code, boolean value ) throws KettleException {
    saveJobEntryAttribute( id_job, id_jobentry, 0, code, value );
  }

  @Override
  public void saveJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, String code, long value ) throws KettleException {
    saveJobEntryAttribute( id_job, id_jobentry, 0, code, value );
  }

  @Override
  public void saveJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, String code, String value ) throws KettleException {
    saveJobEntryAttribute( id_job, id_jobentry, 0, code, value );
  }

  @Override
  public DatabaseMeta loadDatabaseMetaFromJobEntryAttribute( ObjectId id_jobentry, String nameCode, String idCode,
    List<DatabaseMeta> databases ) throws KettleException {
    return loadDatabaseMetaFromJobEntryAttribute( id_jobentry, nameCode, 0, idCode, databases );
  }

  @Override
  public void save( RepositoryElementInterface repoElement, String versionComment, ProgressMonitorListener monitor ) throws KettleException {
    save( repoElement, versionComment, monitor, false );
  }

  @Override
  public void saveDatabaseMetaJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, String nameCode,
    String idCode, DatabaseMeta database ) throws KettleException {
    saveDatabaseMetaJobEntryAttribute( id_job, id_jobentry, 0, nameCode, idCode, database );
  }

  public boolean test() {
    return true;
  }

  public void create() {

  }

  @Override
  public IUnifiedRepository getUnderlyingRepository() {
    return null;
  }

  @Override
  public Bowl getBowl() {
    return bowl;
  }
}
