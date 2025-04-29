package org.pentaho.di.trans;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.util.function.Supplier;

public interface TransFactory {

  Trans create( TransMeta transMeta, LogChannelInterface log, Supplier<Trans> fallbackSupplier ) throws KettleException;
}
