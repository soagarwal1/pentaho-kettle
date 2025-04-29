package org.pentaho.di.trans;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.util.function.Supplier;

public interface TransMananger  {
  Trans createTrans( TransMeta transMeta, LogChannelInterface log, String runConfiguration,
    Supplier<Trans> fallbackSupplier ) throws KettleException;

  void registerFactory(String RunConfigurationType, TransFactory transFactory );

  TransFactory getTransFactory( String RunConfigurationType );
}
