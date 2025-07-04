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


package org.pentaho.di.core.logging;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.core.bowl.DefaultBowl;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.vfs.KettleVFS;

/**
 * This class takes care of polling the central log store for new log messages belonging to a certain log channel ID
 * (and children). The log lines will be written to a logging file.
 *
 * @author matt
 *
 */
public class LogChannelFileWriter {
  private String logChannelId;
  private FileObject logFile;
  private boolean appending;
  private int pollingInterval;

  private AtomicBoolean active;
  private AtomicBoolean finished;

  private KettleException exception;
  protected OutputStream logFileOutputStream;

  private LogChannelFileWriterBuffer buffer;

  /**
   * Create a new log channel file writer
   *
   * @param logChannelId
   *          The log channel (+children) to write to the log file
   * @param logFile
   *          The logging file to write to
   * @param appending
   *          set to true if you want to append to an existing file
   * @param pollingInterval
   *          The polling interval in milliseconds.
   *
   * @throws KettleException
   *           in case the specified log file can't be created.
   */
  public LogChannelFileWriter( String logChannelId, FileObject logFile, boolean appending, int pollingInterval ) throws KettleException {
    this.logChannelId = logChannelId;
    this.logFile = logFile;
    this.appending = appending;
    this.pollingInterval = pollingInterval;

    active = new AtomicBoolean( false );
    finished = new AtomicBoolean( false );

    try {
      logFileOutputStream = KettleVFS.getInstance( DefaultBowl.getInstance() ).getOutputStream( logFile, appending );
    } catch ( IOException e ) {
      throw new KettleException( "There was an error while trying to open file '" + logFile + "' for writing", e );
    }

    this.buffer = new LogChannelFileWriterBuffer( this.logChannelId );
    LoggingRegistry.getInstance().registerLogChannelFileWriterBuffer( this.buffer );
  }

  /**
   * Create a new log channel file writer
   *
   * @param logChannelId
   *          The log channel (+children) to write to the log file
   * @param logFile
   *          The logging file to write to
   * @param appending
   *          set to true if you want to append to an existing file
   *
   * @throws KettleException
   *           in case the specified log file can't be created.
   */
  public LogChannelFileWriter( String logChannelId, FileObject logFile, boolean appending ) throws KettleException {
    this( logChannelId, logFile, appending, 1000 );
  }

  /**
   * Start the logging thread which will write log data from the specified log channel to the log file. In case of an
   * error, the exception will be available with method getException().
   */
  public void startLogging() {

    exception = null;
    active.set( true );

    Thread thread = new Thread( new Runnable() {
      public void run() {
        try {

          while ( active.get() && exception == null ) {
            flush();
            Thread.sleep( pollingInterval );
          }
          // When done, save the last bit as well...
          flush();

        } catch ( Exception e ) {
          exception = new KettleException( "There was an error logging to file '" + logFile + "'", e );
        } finally {
          try {
            if ( logFileOutputStream != null ) {
              logFileOutputStream.close();
              logFileOutputStream = null;
            }

            if ( buffer != null ) {
              LoggingRegistry.getInstance().removeLogChannelFileWriterBuffer( buffer.getLogChannelId() );
            }
          } catch ( Exception e ) {
            exception = new KettleException( "There was an error closing log file file '" + logFile + "'", e );
          } finally {
            finished.set( true );
          }
        }
      }
    } );
    thread.start();
  }

  public synchronized void flush() {
    try {
      StringBuffer buffer = this.buffer.getBuffer();
      logFileOutputStream.write( buffer.toString().getBytes() );
      logFileOutputStream.flush();

    } catch ( Exception e ) {
      exception = new KettleException( "There was an error logging to file '" + logFile + "'", e );
    }
  }

  public void stopLogging() {
    flush();
    active.set( false );
    while ( !finished.get() ) {
      Thread.yield();
    }

    if ( this.buffer != null ) {
      LoggingRegistry.getInstance().removeLogChannelFileWriterBuffer( this.buffer.getLogChannelId() );
    }
  }

  public KettleException getException() {
    return exception;
  }

  /**
   * @return the logChannelId
   */
  public String getLogChannelId() {
    return logChannelId;
  }

  /**
   * @param logChannelId
   *          the logChannelId to set
   */
  public void setLogChannelId( String logChannelId ) {
    this.logChannelId = logChannelId;
  }

  /**
   * @return the logFile
   */
  public FileObject getLogFile() {
    return logFile;
  }

  /**
   * @param logFile
   *          the logFile to set
   */
  public void setLogFile( FileObject logFile ) {
    this.logFile = logFile;
  }

  /**
   * @return the appending
   */
  public boolean isAppending() {
    return appending;
  }

  /**
   * @param appending
   *          the appending to set
   */
  public void setAppending( boolean appending ) {
    this.appending = appending;
  }

  /**
   * @return the pollingInterval
   */
  public int getPollingInterval() {
    return pollingInterval;
  }

  /**
   * @param pollingInterval
   *          the pollingInterval to set
   */
  public void setPollingInterval( int pollingInterval ) {
    this.pollingInterval = pollingInterval;
  }
}
