package org.pentaho.di.trans;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class DefaultTransManager implements TransMananger {

  private static final DefaultTransManager instance = new DefaultTransManager();

  Map<String, TransFactory> mapConfigurationTypeTransFactory;


  public DefaultTransManager(){
    mapConfigurationTypeTransFactory = new HashMap<>();
  }

  public static DefaultTransManager getInstance() {
    return instance;
  }

  @Override
  public Trans createTrans( TransMeta transMeta, LogChannelInterface log, String runConfiguration,
    Supplier<Trans> fallbackSupplier ) throws KettleException {
    TransFactory transFactory = getTransFactory( runConfiguration );
    return transFactory.create( transMeta, log, fallbackSupplier );
  }

  @Override
  public void registerFactory(String runConfigurationType, TransFactory transFactory ) {
    mapConfigurationTypeTransFactory.put(runConfigurationType, transFactory);
  }

  @Override public TransFactory getTransFactory( String runConfigurationType ) {
    return mapConfigurationTypeTransFactory.getOrDefault( runConfigurationType, new TransFactoryImpl() );
  }
}
