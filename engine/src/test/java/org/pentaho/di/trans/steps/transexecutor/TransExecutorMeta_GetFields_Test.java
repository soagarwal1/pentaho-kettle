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


package org.pentaho.di.trans.steps.transexecutor;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.pentaho.di.core.bowl.DefaultBowl;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.di.trans.step.StepMeta;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransExecutorMeta_GetFields_Test {
  @ClassRule public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();

  private TransExecutorMeta meta;

  private StepMeta executionResult;
  private StepMeta resultFiles;
  private StepMeta outputRows;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init( false );
  }

  @Before
  public void setUp() {
    executionResult = mock( StepMeta.class );
    resultFiles = mock( StepMeta.class );
    outputRows = mock( StepMeta.class );

    meta = new TransExecutorMeta();
    meta.setExecutionResultTargetStepMeta( executionResult );
    meta.setResultFilesTargetStepMeta( resultFiles );
    meta.setOutputRowsSourceStepMeta( outputRows );

    meta.setExecutionTimeField( "executionTime" );
    meta.setExecutionResultField( "true" );
    meta.setExecutionNrErrorsField( "1" );

    meta.setResultFilesFileNameField( "resultFileName" );

    meta.setOutputRowsField( new String[] { "outputRow" } );
    meta.setOutputRowsType( new int[] { 0 } );
    meta.setOutputRowsLength( new int[] { 0 } );
    meta.setOutputRowsPrecision( new int[] { 0 } );

    meta = spy( meta );

    StepMeta parent = mock( StepMeta.class );
    doReturn( parent ).when( meta ).getParentStepMeta();
    when( parent.getName() ).thenReturn( "parent step" );

  }

  @Test
  public void getFieldsForExecutionResults() throws Exception {
    RowMetaInterface mock = invokeGetFieldsWith( executionResult );
    verify( mock, times( 3 ) ).addValueMeta( any( ValueMetaInterface.class ) );
  }

  @Test
  public void getFieldsForResultFiles() throws Exception {
    RowMetaInterface mock = invokeGetFieldsWith( resultFiles );
    verify( mock ).addValueMeta( any( ValueMetaInterface.class ) );
  }

  @Test
  public void getFieldsForInternalTransformationOutputRows() throws Exception {
    RowMetaInterface mock = invokeGetFieldsWith( outputRows );
    verify( mock ).addValueMeta( any( ValueMetaInterface.class ) );
  }

  private RowMetaInterface invokeGetFieldsWith( StepMeta stepMeta ) throws Exception {
    RowMetaInterface rowMeta = mock( RowMetaInterface.class );
    meta.getFields( DefaultBowl.getInstance(), rowMeta, "", null, stepMeta, null, null, null );
    return rowMeta;
  }
}
