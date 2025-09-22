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


package org.pentaho.di.pan;

import org.pentaho.di.base.AbstractBaseCommandExecutor;
import org.pentaho.di.base.CommandExecutorCodes;
import org.pentaho.di.base.Params;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.parameters.UnknownParamException;
import org.pentaho.di.core.util.FileUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.RepositoriesMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.w3c.dom.Document;

import java.io.File;
import java.io.InputStream;
import java.io.Serializable;


public abstract class PanCommandExecutor extends AbstractBaseCommandExecutor {

  public abstract Result execute( final Params params, String[] arguments ) throws Exception;

  public int printVersion() {
    printVersion( "Pan.Log.KettleVersion" );
    return CommandExecutorCodes.Pan.KETTLE_VERSION_PRINT.getCode();
  }

  protected void executeRepositoryBasedCommand( Repository repository, String dirName, String listTrans,
                                                String listDirs, String exportRepo ) throws KettleException {

    RepositoryDirectoryInterface directory = loadRepositoryDirectory( repository, dirName, "Pan.Error.NoRepProvided",
      "Pan.Log.Allocate&ConnectRep", "Pan.Error.CanNotFindSpecifiedDirectory" );

    if ( directory == null ) {
      return; // not much we can do here
    }

    if ( isEnabled( listTrans ) ) {
      printRepositoryStoredTransformations( repository, directory ); // List the transformations in the repository

    } else if ( isEnabled( listDirs ) ) {
      printRepositoryDirectories( repository, directory ); // List the directories in the repository

    } else if ( !Utils.isEmpty( exportRepo ) ) {
      // Export the repository
      System.out.println(
        BaseMessages.getString( getPkgClazz(), "Pan.Log.ExportingObjectsRepToFile", "" + exportRepo ) );
      repository.getExporter().exportAllObjects( null, exportRepo, directory, "all" );
      System.out.println(
        BaseMessages.getString( getPkgClazz(), "Pan.Log.FinishedExportObjectsRepToFile", "" + exportRepo ) );
    }
  }

  public Trans loadTransFromRepository( Repository repository, String dirName, String transName ) throws KettleException {

    if ( Utils.isEmpty( transName ) ) {
      System.out.println( BaseMessages.getString( getPkgClazz(), "Pan.Error.NoTransNameSupplied" ) );
      return null;
    }

    RepositoryDirectoryInterface directory = loadRepositoryDirectory( repository, dirName, "Pan.Error.NoRepProvided",
      "Pan.Log.Allocate&ConnectRep", "Pan.Error.CanNotFindSpecifiedDirectory" );

    if ( directory == null ) {
      return null; // not much we can do here
    }

    logDebug( "Pan.Log.LoadTransInfo" );
    TransMeta transMeta = repository.loadTransformation( transName, directory, null, true, null );

    logDebug( "Pan.Log.AllocateTrans" );
    Trans trans = new Trans( transMeta );
    trans.setRepository( repository );
    trans.setMetaStore( getMetaStore() );

    return trans; // return transformation loaded from the repo
  }

  public Trans loadTransFromFilesystem( String initialDir, String filename, String jarFilename,
    Serializable base64Zip ) throws Exception {

    Trans trans = null;

    File zip;
    if ( base64Zip != null && ( zip = decodeBase64ToZipFile( base64Zip, true ) ) != null ) {
      // update filename to a meaningful, 'ETL-file-within-zip' syntax
      filename = "zip:file:" + File.separator + File.separator + zip.getAbsolutePath() + "!" + filename;
    }

    // Try to load the transformation from file
    if ( !Utils.isEmpty( filename ) ) {

      String filepath = filename;
      // If the filename starts with scheme like zip:, then isAbsolute() will return false even though the
      // the path following the zip is absolute. Check for isAbsolute only if the fileName does not start with scheme
      if ( !KettleVFS.startsWithScheme( filename ) && !FileUtil.isFullyQualified( filename ) ) {
        filepath = initialDir + filename;
      }

      logDebug( "Pan.Log.LoadingTransXML", "" + filepath );
      TransMeta transMeta = new TransMeta( getBowl(), filepath );
      trans = new Trans( transMeta );
    }

    if ( !Utils.isEmpty( jarFilename ) ) {

      try {

        logDebug( "Pan.Log.LoadingTransJar", jarFilename );

        InputStream inputStream = PanCommandExecutor.class.getResourceAsStream( jarFilename );
        StringBuilder xml = new StringBuilder();
        int c;
        while ( ( c = inputStream.read() ) != -1 ) {
          xml.append( (char) c );
        }
        inputStream.close();
        Document document = XMLHandler.loadXMLString( xml.toString() );
        TransMeta transMeta = new TransMeta( XMLHandler.getSubNode( document, "transformation" ), null );
        trans = new Trans( transMeta );

      } catch ( Exception e ) {

        System.out.println( BaseMessages.getString( getPkgClazz(), "Pan.Error.ReadingJar", e.toString() ) );
        System.out.println( Const.getStackTracker( e ) );
        throw e;
      }
    }

    if ( trans != null ) {
      trans.setMetaStore( getMetaStore() );
    }

    return trans;
  }

  /**
   * Configures the transformation with the given parameters and their values
   *
   * @param trans        the executable transformation object
   * @param optionParams the list of parameters to set for the transformation
   * @param transMeta    the transformation metadata
   * @throws UnknownParamException
   */
  protected static void configureParameters( Trans trans, NamedParams optionParams,
                                             TransMeta transMeta ) throws UnknownParamException {
    trans.initializeVariablesFrom( null );
    trans.getTransMeta().setInternalKettleVariables( trans );

    // Map the command line named parameters to the actual named parameters.
    // Skip for the moment any extra command line parameter not known in the transformation.
    String[] transParams = trans.listParameters();
    for ( String param : transParams ) {
      String value = optionParams.getParameterValue( param );
      if ( value != null ) {
        trans.setParameterValue( param, value );
        transMeta.setParameterValue( param, value );
      }
    }

    // Put the parameters over the already defined variable space. Parameters get priority.
    trans.activateParameters();
  }

  protected void printTransformationParameters( Trans trans ) throws UnknownParamException {

    if ( trans != null && trans.listParameters() != null ) {

      for ( String pName : trans.listParameters() ) {
        printParameter( pName, trans.getParameterValue( pName ), trans.getParameterDefault( pName ),
          trans.getParameterDescription( pName ) );
      }
    }
  }

  protected void printRepositoryStoredTransformations( Repository repository, RepositoryDirectoryInterface directory )
    throws KettleException {

    logDebug( "Pan.Log.GettingListTransDirectory", "" + directory );
    String[] transformations = repository.getTransformationNames( directory.getObjectId(), false );

    if ( transformations != null ) {
      for ( String trans : transformations ) {
        System.out.println( trans );
      }
    }
  }

  protected void printRepositories( RepositoriesMeta repositoriesMeta ) {

    if ( repositoriesMeta != null ) {

      logDebug( "Pan.Log.GettingListReps" );

      for ( int i = 0; i < repositoriesMeta.nrRepositories(); i++ ) {
        RepositoryMeta repInfo = repositoriesMeta.getRepository( i );
        System.out.println( BaseMessages.getString( getPkgClazz(), "Pan.Log.RepNameDesc", "" + ( i + 1 ),
          repInfo.getName(), repInfo.getDescription() ) );
      }
    }
  }
}


