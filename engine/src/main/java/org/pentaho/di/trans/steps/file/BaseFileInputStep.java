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


package org.pentaho.di.trans.steps.file;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.ResultFile;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.errorhandling.CompositeFileErrorHandler;
import org.pentaho.di.trans.step.errorhandling.FileErrorHandler;
import org.pentaho.di.trans.step.errorhandling.FileErrorHandlerContentLineNumber;
import org.pentaho.di.trans.step.errorhandling.FileErrorHandlerMissingFiles;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class contains base functionality for file-based input steps.
 *
 * @author Alexander Buloichik
 */
public abstract class BaseFileInputStep<M extends BaseFileInputMeta<?, ?, ?>, D extends BaseFileInputStepData> extends
    BaseStep implements IBaseFileInputStepControl {
  private static Class<?> PKG = BaseFileInputStep.class; // for i18n purposes, needed by Translator2!!

  protected M meta;

  protected D data;

  /**
   * Content-dependent initialization.
   */
  protected abstract boolean init();

  /**
   * Create reader for specific file.
   */
  protected abstract IBaseFileInputReader createReader( M meta, D data, FileObject file ) throws Exception;

  public BaseFileInputStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  /**
   * Initialize step before execute.
   */
  @Override
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (M) smi;
    data = (D) sdi;

    if ( !super.init( smi, sdi ) ) {
      return false;
    }

    //Set Embedded NamedCluter MetatStore Provider Key so that it can be passed to VFS
    if ( getTransMeta().getNamedClusterEmbedManager() != null ) {
      getTransMeta().getNamedClusterEmbedManager()
        .passEmbeddedMetastoreKey( this, getTransMeta().getEmbeddedMetastoreProviderKey() );
    }

    initErrorHandling();

    meta.additionalOutputFields.normalize();
    data.files = meta.getFileInputList( getTransMeta().getBowl(), this );
    data.currentFileIndex = 0;

    // If there are missing files,
    // fail if we don't ignore errors
    //
    Result previousResult = getTrans().getPreviousResult();
    Map<String, ResultFile> resultFiles = ( previousResult != null ) ? previousResult.getResultFiles() : null;

    if ( ( previousResult == null || resultFiles == null || resultFiles.size() == 0 ) && data.files
        .nrOfMissingFiles() > 0 && !meta.inputFiles.acceptingFilenames && !meta.errorHandling.errorIgnored ) {
      logError( BaseMessages.getString( PKG, "BaseFileInputStep.Log.Error.NoFilesSpecified" ) );
      return false;
    }

    String clusterSize = getVariable( Const.INTERNAL_VARIABLE_CLUSTER_SIZE );
    if ( !Utils.isEmpty( clusterSize ) && Integer.valueOf( clusterSize ) > 1 ) {
      // TODO: add metadata to configure this.
      String nr = getVariable( Const.INTERNAL_VARIABLE_SLAVE_SERVER_NUMBER );
      if ( log.isDetailed() ) {
        logDetailed( "Running on slave server #" + nr
            + " : assuming that each slave reads a dedicated part of the same file(s)." );
      }
    }

    return init();
  }

  /**
   * Open next VFS file for processing.
   *
   * This method will support different parallelization methods later.
   */
  protected boolean openNextFile() {
    try {
      if ( data.currentFileIndex >= data.files.nrOfFiles() ) {
        // all files already processed
        return false;
      }

      // Is this the last file?
      data.file = data.files.getFile( data.currentFileIndex );
      data.filename = KettleVFS.getFilename( data.file );

      fillFileAdditionalFields( data, data.file );
      if ( meta.inputFiles.passingThruFields ) {
        StringBuilder sb = new StringBuilder();
        sb.append( data.currentFileIndex ).append( "_" ).append( data.file );
        data.currentPassThruFieldsRow = data.passThruFields.get( sb.toString() );
      }

      // Add this files to the result of this transformation.
      //
      if ( meta.inputFiles.isaddresult ) {
        ResultFile resultFile =
            new ResultFile( ResultFile.FILE_TYPE_GENERAL, data.file, getTransMeta().getName(), toString() );
        resultFile.setComment( "File was read by an Text File input step" );
        addResultFile( resultFile );
      }
      if ( log.isBasic() ) {
        logBasic( "Opening file: " + data.file.getName().getFriendlyURI() );
      }

      data.dataErrorLineHandler.handleFile( data.file );

      data.reader = createReader( meta, data, data.file );
    } catch ( Exception e ) {
      if ( !handleOpenFileException( e ) ) {
        return false;
      }
      data.reader = null;
    }
    // Move file pointer ahead!
    data.currentFileIndex++;

    return true;
  }

  protected boolean handleOpenFileException( Exception e ) {
    String errorMsg =
      "Couldn't open file #" + data.currentFileIndex + " : " + data.file.getName().getFriendlyURI();
    if ( !failAfterBadFile( errorMsg ) ) {
      return true;
    }
    stopAll();
    setErrors( getErrors() + 1 );
    logError( errorMsg, e );
    return false;
  }

  /**
   * Process next row. This methods opens next file automatically.
   */
  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (M) smi;
    data = (D) sdi;

    if ( first ) {
      first = false;
      prepareToRowProcessing();

      if ( !openNextFile() ) {
        setOutputDone(); // signal end to receiver(s)
        closeLastFile();
        return false;
      }
    }

    while ( true ) {
      if ( data.reader != null && data.reader.readRow() ) {
        // row processed
        return true;
      }
      // end of current file
      closeLastFile();

      if ( !openNextFile() ) {
        // there are no more files
        break;
      }
    }

    // after all files processed
    setOutputDone(); // signal end to receiver(s)
    closeLastFile();
    return false;
  }

  /**
   * Prepare to process. Executed only first time row processing. It can't be possible to prepare to process in the
   * init() phrase, because files can be in fields from previous step.
   */
  protected void prepareToRowProcessing() throws KettleException {
    data.outputRowMeta = new RowMeta();
    RowMetaInterface[] infoStep = null;

    if ( meta.inputFiles.acceptingFilenames ) {
      // input files from previous step
      infoStep = filesFromPreviousStep();
    }

    // get the metadata populated. Simple and easy.
    meta.getFields( getTransMeta().getBowl(), data.outputRowMeta, getStepname(), infoStep, null, this, repository,
      metaStore );
    // Create convert meta-data objects that will contain Date & Number formatters
    //
    data.convertRowMeta = data.outputRowMeta.cloneToType( ValueMetaInterface.TYPE_STRING );

    BaseFileInputStepUtils.handleMissingFiles( data.files, log, meta.errorHandling.errorIgnored,
        data.dataErrorLineHandler );

    // Count the number of repeat fields...
    for ( int i = 0; i < meta.inputFields.length; i++ ) {
      if ( meta.inputFields[i].isRepeated() ) {
        data.nr_repeats++;
      }
    }
  }

  @Override
  public boolean checkFeedback( long lines ) {
    return super.checkFeedback( lines );
  }

  /**
   * Initialize error handling.
   *
   * TODO: should we set charset for error files from content meta ? What about case for automatic charset ?
   */
  private void initErrorHandling() {
    List<FileErrorHandler> dataErrorLineHandlers = new ArrayList<>( 2 );
    if ( meta.errorHandling.lineNumberFilesDestinationDirectory != null ) {
      dataErrorLineHandlers.add( new FileErrorHandlerContentLineNumber( getTrans().getCurrentDate(),
          environmentSubstitute( meta.errorHandling.lineNumberFilesDestinationDirectory ),
          meta.errorHandling.lineNumberFilesExtension, meta.getEncoding(), this ) );
    }
    if ( meta.errorHandling.errorFilesDestinationDirectory != null ) {
      dataErrorLineHandlers.add( new FileErrorHandlerMissingFiles( getTrans().getCurrentDate(), environmentSubstitute(
          meta.errorHandling.errorFilesDestinationDirectory ), meta.errorHandling.errorFilesExtension, meta
              .getEncoding(), this ) );
    }
    data.dataErrorLineHandler = new CompositeFileErrorHandler( dataErrorLineHandlers );
  }

  /**
   * Read files from previous step.
   */
  private RowMetaInterface[] filesFromPreviousStep() throws KettleException {
    RowMetaInterface[] infoStep = null;
    data.files.getFiles().clear();

    int idx = -1;
    RowSet rowSet = findInputRowSet( meta.inputFiles.acceptingStepName );

    Object[] fileRow = getRowFrom( rowSet );
    while ( fileRow != null ) {
      RowMetaInterface prevInfoFields = rowSet.getRowMeta();
      if ( idx < 0 ) {
        if ( meta.inputFiles.passingThruFields ) {
          data.passThruFields = new HashMap<>();
          infoStep = new RowMetaInterface[] { prevInfoFields };
          data.nrPassThruFields = prevInfoFields.size();
        }
        idx = prevInfoFields.indexOfValue( meta.inputFiles.acceptingField );
        if ( idx < 0 ) {
          logError( BaseMessages.getString( PKG, "BaseFileInputStep.Log.Error.UnableToFindFilenameField",
            meta.inputFiles.acceptingField ) );
          setErrors( getErrors() + 1 );
          stopAll();
          return null;
        }
      }
      String fileValue = prevInfoFields.getString( fileRow, idx );
      try {
        FileObject parentFileObject = KettleVFS.getInstance( getTransMeta().getBowl() )
          .getFileObject( environmentSubstitute( fileValue ), getTransMeta() );
        boolean isDir = ( parentFileObject != null && parentFileObject.getType() == FileType.FOLDER );
        int startingIndex = data.files.nrOfFiles();

        if ( isDir ) { // it's a directory
          addFilesFromFolder( parentFileObject );
        } else {  // it's a file
          data.files.addFile( parentFileObject );
        }

        if ( meta.inputFiles.passingThruFields ) {
          StringBuilder sb = new StringBuilder();

          // PDI-18818 + BACKLOG-34414
          // For directories, the passThruFields should match the same row's value for each file within the directory,
          // For a file, the passThruFields should match the current row's value.
          if ( isDir ) {
            for ( int fileIndex = startingIndex; fileIndex < data.files.nrOfFiles(); fileIndex++ ) {
              FileObject file = data.files.getFile( fileIndex );
              sb.setLength( 0 );
              sb.append( fileIndex ).append( "_" ).append( file != null ? file.toString() : "" );
              data.passThruFields.put( sb.toString(), fileRow );
            }
          } else {
            sb.append( startingIndex ).append( "_" ).append(
                    parentFileObject != null ? parentFileObject.toString() : "" );
            data.passThruFields.put( sb.toString(), fileRow );
          }

        }
      } catch ( FileSystemException e ) {
        logError( BaseMessages.getString( PKG, "BaseFileInputStep.Log.Error.UnableToCreateFileObject", fileValue ), e );
        throw new KettleException( e );
      }

      // Grab another row
      fileRow = getRowFrom( rowSet );
    }

    if ( data.files.nrOfFiles() == 0 ) {
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "BaseFileInputStep.Log.Error.NoFilesSpecified" ) );
      }
      return null;
    }
    return infoStep;
  }

  /**
   * Read the list of files recursively from a given folder
   * @param parentFileObject
   * @throws FileSystemException
   */
  private void addFilesFromFolder( FileObject parentFileObject ) throws FileSystemException {
    final boolean subdirs = true;
    FileObject[] fileObjects = parentFileObject.findFiles( new AllFileSelector() {
      @Override
      public boolean traverseDescendents( FileSelectInfo info ) {
        return info.getDepth() == 0 || subdirs;
      }

      @Override
      public boolean includeFile( FileSelectInfo info ) {
        // Never return the parent directory of a file list.
        if ( info.getDepth() == 0 ) {
          return false;
        }

        FileObject fileObject = info.getFile();
        try {
          return fileObject != null && FileType.FILE.equals( fileObject.getType() );
        } catch ( Exception ex ) {
          // Upon error don't process the file.
          return false;
        }
      }
    } );
    if ( fileObjects != null ) {
      for ( int j = 0; j < fileObjects.length; j++ ) {
        FileObject fileObject = fileObjects[ j ];
        if ( fileObject.exists() ) {
          data.files.addFile( fileObject );
        }
      }
    }
    if ( Utils.isEmpty( fileObjects ) ) {
      data.files.addNonAccessibleFile( parentFileObject );
    }
  }
  /**
   * Close last opened file/
   */
  protected void closeLastFile() {
    if ( data.reader != null ) {
      try {
        data.reader.close();
      } catch ( Exception ex ) {
        failAfterBadFile( "Error close reader" );
      }
      data.reader = null;
    }
    if ( data.file != null ) {
      try {
        data.file.close();
      } catch ( Exception ex ) {
        failAfterBadFile( "Error close file" );
      }
      data.file = null;
    }
  }

  /**
   * Dispose step.
   */
  @Override
  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    closeLastFile();

    super.dispose( smi, sdi );
  }

  /**
   *
   * @param errorMsg
   *          Message to send to rejected row if enabled
   * @return If should stop processing after having problems with a file
   */
  public boolean failAfterBadFile( String errorMsg ) {

    if ( getStepMeta().isDoingErrorHandling() && data.filename != null && !data.rejectedFiles.containsKey(
        data.filename ) ) {
      data.rejectedFiles.put( data.filename, true );
      rejectCurrentFile( errorMsg );
    }

    return !meta.errorHandling.errorIgnored || !meta.errorHandling.skipBadFiles;
  }

  /**
   * Send file name and/or error message to error output
   *
   * @param errorMsg
   *          Message to send to rejected row if enabled
   */
  private void rejectCurrentFile( String errorMsg ) {
    if ( StringUtils.isNotBlank( meta.errorHandling.fileErrorField ) || StringUtils.isNotBlank(
        meta.errorHandling.fileErrorMessageField ) ) {
      RowMetaInterface rowMeta = getInputRowMeta();
      if ( rowMeta == null ) {
        rowMeta = new RowMeta();
      }

      int errorFileIndex =
          ( StringUtils.isBlank( meta.errorHandling.fileErrorField ) ) ? -1 : BaseFileInputStepUtils.addValueMeta(
              getStepname(), rowMeta, this.environmentSubstitute( meta.errorHandling.fileErrorField ) );

      int errorMessageIndex =
          StringUtils.isBlank( meta.errorHandling.fileErrorMessageField ) ? -1 : BaseFileInputStepUtils.addValueMeta(
              getStepname(), rowMeta, this.environmentSubstitute( meta.errorHandling.fileErrorMessageField ) );

      try {
        Object[] rowData = getRow();
        if ( rowData == null ) {
          rowData = RowDataUtil.allocateRowData( rowMeta.size() );
        }

        if ( errorFileIndex >= 0 ) {
          rowData[errorFileIndex] = data.filename;
        }
        if ( errorMessageIndex >= 0 ) {
          rowData[errorMessageIndex] = errorMsg;
        }

        putError( rowMeta, rowData, getErrors(), data.filename, null, "ERROR_CODE" );
      } catch ( Exception e ) {
        logError( "Error sending error row", e );
      }
    }
  }

  /**
   * Prepare file-dependent data for fill additional fields.
   */
  protected void fillFileAdditionalFields( D data, FileObject file ) throws FileSystemException {
    data.shortFilename = file.getName().getBaseName();
    data.path = KettleVFS.getFilename( file.getParent() );
    data.hidden = file.isHidden();
    data.extension = file.getName().getExtension();
    data.uriName = Const.optionallyDecodeUriString( file.getName().getURI() );
    data.rootUriName = file.getName().getRootURI();
    if ( file.getType().hasContent() ) {
      data.lastModificationDateTime = new Date( file.getContent().getLastModifiedTime() );
      data.size = file.getContent().getSize();
    } else {
      data.lastModificationDateTime = null;
      data.size = null;
    }
  }
}
