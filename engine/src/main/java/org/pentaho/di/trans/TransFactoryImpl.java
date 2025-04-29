package org.pentaho.di.trans;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.util.function.Supplier;

public class TransFactoryImpl implements TransFactory {
  @Override
  public Trans create( TransMeta transMeta, LogChannelInterface log, Supplier<Trans> fallbackSupplier )
    throws KettleException {
    return fallbackSupplier.get();
  }
}
