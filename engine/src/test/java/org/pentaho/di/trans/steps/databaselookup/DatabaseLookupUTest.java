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


package org.pentaho.di.trans.steps.databaselookup;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.bowl.Bowl;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.database.MySQLDatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBinary;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.mock.StepMockHelper;
import org.pentaho.metastore.api.IMetaStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.test.util.InternalState.setInternalState;

/**
 * @author Andrey Khayrutdinov
 */
public class DatabaseLookupUTest {
  @ClassRule public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();

  private static final String BINARY_FIELD = "aBinaryFieldInDb";
  private static final String ID_FIELD = "id";
  private StepMockHelper<DatabaseLookupMeta, DatabaseLookupData> mockHelper;

  @BeforeClass
  public static void setUpClass() throws Exception {
    KettleEnvironment.init();
  }

  @AfterClass
  public static void tearDown() {
    KettleEnvironment.reset();
  }

  @Before
  public void setUp() {
    mockHelper = createMockHelper();
  }

  @After
  public void cleanUp() {
    mockHelper.cleanUp();
  }

  @Test
  public void mySqlVariantDbIsLazyConverted() throws Exception {
    DatabaseLookupMeta meta = createDatabaseMeta();
    DatabaseLookupData data = createDatabaseData();
    Database db = createVirtualDb( meta.getDatabaseMeta() );

    DatabaseLookup lookup = spyLookup( mockHelper, db, meta.getDatabaseMeta() );

    lookup.init( meta, data );
    lookup.processRow( meta, data );

    verify( db ).connect( nullable( String.class ) );
  }


  private StepMockHelper<DatabaseLookupMeta, DatabaseLookupData> createMockHelper() {
    StepMockHelper<DatabaseLookupMeta, DatabaseLookupData> ret =
      new StepMockHelper<>( "test DatabaseLookup", DatabaseLookupMeta.class, DatabaseLookupData.class );
    when( ret.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) )
      .thenReturn( ret.logChannelInterface );
    when( ret.trans.isRunning() ).thenReturn( true );

    RowSet rowSet = mock( RowSet.class );
    when( rowSet.getRowWait( nullable( Long.class ), nullable( TimeUnit.class ) ) ).thenReturn( new Object[ 0 ] )
      .thenReturn( null );
    when( rowSet.getRowMeta() ).thenReturn( new RowMeta() );

    when( ret.trans.findRowSet( nullable( String.class ), nullable( Integer.class ), nullable( String.class ),
      nullable( Integer.class ) ) ).thenReturn( rowSet );

    when( ret.transMeta.findNextSteps( ArgumentMatchers.any( StepMeta.class ) ) )
      .thenReturn( Collections.singletonList( mock( StepMeta.class ) ) );
    when( ret.transMeta.findPreviousSteps( any( StepMeta.class ), anyBoolean() ) )
      .thenReturn( Collections.singletonList( mock( StepMeta.class ) ) );

    return ret;
  }

  private DatabaseLookupMeta createDatabaseMeta() throws KettleException {
    MySQLDatabaseMeta mysql = new MySQLDatabaseMeta();
    mysql.setName( "MySQL" );
    DatabaseMeta dbMeta = new DatabaseMeta();
    dbMeta.setDatabaseInterface( mysql );

    DatabaseLookupMeta meta = new DatabaseLookupMeta();
    meta.setDatabaseMeta( dbMeta );
    meta.setTablename( "VirtualTable" );

    meta.setTableKeyField( new String[] { ID_FIELD } );
    meta.setKeyCondition( new String[] { "=" } );

    meta.setReturnValueNewName( new String[] { "returned value" } );
    meta.setReturnValueField( new String[] { BINARY_FIELD } );
    meta.setReturnValueDefaultType( new int[] { ValueMetaInterface.TYPE_BINARY } );

    meta.setStreamKeyField1( new String[ 0 ] );
    meta.setStreamKeyField2( new String[ 0 ] );

    meta.setReturnValueDefault( new String[] { Const.EMPTY_STRING } );

    meta = spy( meta );
    doAnswer( invocation -> {
      RowMetaInterface row = (RowMetaInterface) invocation.getArguments()[ 0 ];
      row.addValueMeta( new ValueMetaBinary( BINARY_FIELD ) );
      return null;
    } ).when( meta ).getFields(
      any( Bowl.class ),
      any( RowMetaInterface.class ),
      anyString(),
      any( RowMetaInterface[].class ),
      any( StepMeta.class ),
      any( VariableSpace.class ),
      any( Repository.class ),
      any( IMetaStore.class ) );

    return meta;
  }

  private DatabaseLookupData createDatabaseData() {
    return new DatabaseLookupData();
  }

  private Database createVirtualDb( DatabaseMeta meta ) throws Exception {
    ResultSet rs = mock( ResultSet.class );
    when( rs.getMetaData() ).thenReturn( mock( ResultSetMetaData.class ) );

    PreparedStatement ps = mock( PreparedStatement.class );
    when( ps.executeQuery() ).thenReturn( rs );

    Connection connection = mock( Connection.class );
    when( connection.prepareStatement( anyString() ) ).thenReturn( ps );

    Database db = new Database( mock( LoggingObjectInterface.class ), meta );
    db.setConnection( connection );

    db = spy( db );
    doNothing().when( db ).normalConnect( nullable( String.class ) );

    ValueMetaInterface binary = new ValueMetaString( BINARY_FIELD );
    binary.setStorageType( ValueMetaInterface.STORAGE_TYPE_BINARY_STRING );

    ValueMetaInterface id = new ValueMetaInteger( ID_FIELD );

    RowMetaInterface metaByQuerying = new RowMeta();
    metaByQuerying.addValueMeta( binary );
    metaByQuerying.addValueMeta( id );

    doReturn( metaByQuerying ).when( db ).getTableFields( anyString() );
    doReturn( metaByQuerying ).when( db ).getTableFieldsMeta( anyString(), anyString() );

    return db;
  }

  private DatabaseLookup spyLookup( StepMockHelper<DatabaseLookupMeta, DatabaseLookupData> mocks, Database db,
                                    DatabaseMeta dbMeta ) {
    DatabaseLookup lookup =
      spy( new DatabaseLookup( mocks.stepMeta, mocks.stepDataInterface, 1, mocks.transMeta, mocks.trans ) );

    doReturn( db ).when( lookup ).getDatabase( dbMeta );
    for ( RowSet rowSet : lookup.getOutputRowSets() ) {
      if ( mockingDetails( rowSet ).isMock() ) {
        when( rowSet.putRow( any( RowMetaInterface.class ), any( Object[].class ) ) ).thenReturn( true );
      }
    }

    return lookup;
  }

  @Test
  public void testEqualsAndIsNullAreCached() throws Exception {
    when( mockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) )
      .thenReturn( mockHelper.logChannelInterface );

    DatabaseLookup look =
      new MockDatabaseLookup( mockHelper.stepMeta, mockHelper.stepDataInterface, 0, mockHelper.transMeta,
        mockHelper.trans );
    DatabaseLookupData lookData = new DatabaseLookupData();
    lookData.cache = DefaultCache.newCache( lookData, 0 );
    lookData.lookupMeta = new RowMeta();

    MySQLDatabaseMeta mysql = new MySQLDatabaseMeta();
    mysql.setName( "MySQL" );
    DatabaseMeta dbMeta = new DatabaseMeta();
    dbMeta.setDatabaseInterface( mysql );

    DatabaseLookupMeta meta = new DatabaseLookupMeta();
    meta.setDatabaseMeta( dbMeta );
    meta.setTablename( "VirtualTable" );

    meta.setTableKeyField( new String[] { "ID1", "ID2" } );
    meta.setKeyCondition( new String[] { "=", "IS NULL" } );

    meta.setReturnValueNewName( new String[] { "val1", "val2" } );
    meta.setReturnValueField( new String[] { BINARY_FIELD, BINARY_FIELD } );
    meta.setReturnValueDefaultType( new int[] { ValueMetaInterface.TYPE_BINARY, ValueMetaInterface.TYPE_BINARY } );

    meta.setStreamKeyField1( new String[ 0 ] );
    meta.setStreamKeyField2( new String[ 0 ] );

    meta.setReturnValueDefault( new String[] { Const.EMPTY_STRING, Const.EMPTY_STRING } );

    meta = spy( meta );
    doAnswer( invocation -> {
      RowMetaInterface row = (RowMetaInterface) invocation.getArguments()[ 0 ];
      ValueMetaInterface v = new ValueMetaBinary( BINARY_FIELD );
      row.addValueMeta( v );
      return null;
    } ).when( meta ).getFields(
      any( Bowl.class ),
      any( RowMetaInterface.class ),
      anyString(),
      any( RowMetaInterface[].class ),
      any( StepMeta.class ),
      any( VariableSpace.class ),
      any( Repository.class ),
      any( IMetaStore.class ) );

    look.init( meta, lookData );
    assertTrue( lookData.allEquals ); // Test for fix on PDI-15202
  }

  @Test
  public void getRowInCacheTest() throws KettleException {
    when( mockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) )
      .thenReturn( mockHelper.logChannelInterface );

    DatabaseLookup look =
      new DatabaseLookup( mockHelper.stepMeta, mockHelper.stepDataInterface, 0, mockHelper.transMeta,
        mockHelper.trans );
    DatabaseLookupData lookData = new DatabaseLookupData();
    lookData.cache = DefaultCache.newCache( lookData, 0 );
    lookData.lookupMeta = new RowMeta();

    look.init( new DatabaseLookupMeta(), lookData );

    ValueMetaInterface valueMeta = new ValueMetaInteger( "fieldTest" );
    RowMeta lookupMeta = new RowMeta();
    lookupMeta.setValueMetaList( Collections.singletonList( valueMeta ) );
    Object[] kgsRow1 = new Object[] { 1L };
    Object[] kgsRow2 = new Object[] { 2L };
    Object[] add1 = new Object[] { 10L };
    Object[] add2 = new Object[] { 20L };
    lookData.cache.storeRowInCache( mockHelper.processRowsStepMetaInterface, lookupMeta, kgsRow1, add1 );
    lookData.cache.storeRowInCache( mockHelper.processRowsStepMetaInterface, lookupMeta, kgsRow2, add2 );

    lookData.conditions = new int[] { DatabaseLookupMeta.CONDITION_GE };
    Object[] rowToCache = new Object[] { 0L };
    Object[] dataFromCache = lookData.cache.getRowFromCache( lookupMeta, rowToCache );

    assertArrayEquals( add1, dataFromCache );
  }

  @Test
  public void createsReadOnlyCache_WhenReadAll_AndNotAllEquals() throws Exception {
    DatabaseLookupData data = getCreatedData( false );
    assertNotNull( data );
  }

  @Test
  public void createsReadDefaultCache_WhenReadAll_AndAllEquals() throws Exception {
    DatabaseLookupData data = getCreatedData( true );
    assertNotNull( data );
  }

  private DatabaseLookupData getCreatedData( boolean allEquals ) throws Exception {
    Database db = mock( Database.class );
    when( db.getRows( anyString(), anyInt() ) )
      .thenReturn( Collections.singletonList( new Object[] { 1L } ) );

    RowMeta returnRowMeta = new RowMeta();
    returnRowMeta.addValueMeta( new ValueMetaInteger() );
    when( db.getReturnRowMeta() ).thenReturn( returnRowMeta );

    DatabaseLookupMeta meta = createTestMeta();
    DatabaseLookupData data = new DatabaseLookupData();

    DatabaseLookup step = createSpiedStep( db, mockHelper, meta );
    step.init( meta, data );


    data.db = db;
    data.keytypes = new int[] { ValueMetaInterface.TYPE_INTEGER };
    if ( allEquals ) {
      data.allEquals = true;
      data.conditions = new int[] { DatabaseLookupMeta.CONDITION_EQ };
    } else {
      data.allEquals = false;
      data.conditions = new int[] { DatabaseLookupMeta.CONDITION_LT };
    }
    step.processRow( meta, data );

    return data;
  }

  private DatabaseLookupMeta createTestMeta() {
    DatabaseLookupMeta meta = new DatabaseLookupMeta();
    meta.setCached( true );
    meta.setLoadingAllDataInCache( true );
    meta.setDatabaseMeta( mock( DatabaseMeta.class ) );
    // it's ok here, we won't do actual work
    meta.allocate( 1, 0 );
    meta.setStreamKeyField1( new String[] { "Test" } );
    return meta;
  }

  private DatabaseLookup createSpiedStep( Database db,
                                          StepMockHelper<DatabaseLookupMeta, DatabaseLookupData> mockHelper,
                                          DatabaseLookupMeta meta ) throws KettleException {
    DatabaseLookup step = spyLookup( mockHelper, db, meta.getDatabaseMeta() );
    doNothing().when( step ).determineFieldsTypesQueryingDb();
    doReturn( null ).when( step ).lookupValues( nullable( RowMetaInterface.class ), nullable( Object[].class ) );

    RowMeta input = new RowMeta();
    input.addValueMeta( new ValueMetaInteger( "Test" ) );
    doCallRealMethod().when( step ).setInputRowMeta( any() );
    step.setInputRowMeta( input );
    return step;
  }


  @Test
  public void createsReadDefaultCache_AndUsesOnlyNeededFieldsFromMeta() throws Exception {
    Database db = mock( Database.class );

    doCallRealMethod().when( db ).forEachRow( anyString(), anyInt(), any( Consumer.class ) );
    ResultSet resultSetMock = mock( ResultSet.class );
    doReturn( resultSetMock ).when( db ).openQuery( anyString() );
    doReturn( new Object[] { 1L, 10L }, new Object[] { 2L, 20L }, new Object[] { 3L, 30L },
      null ).when( db ).getRow( resultSetMock );

    doNothing().when( db ).connect();
    doNothing().when( db ).connect( any() );

    RowMeta returnRowMeta = new RowMeta();
    returnRowMeta.addValueMeta( new ValueMetaInteger("i1") );
    returnRowMeta.addValueMeta( new ValueMetaInteger("i2") );
    doReturn( returnRowMeta ).when( db ).getTableFields( nullable( String.class ) );
    doReturn( returnRowMeta ).when( db ).getTableFieldsMeta( nullable( String.class ), nullable( String.class ) );

    when( db.getReturnRowMeta() ).thenReturn( returnRowMeta );

    DatabaseLookupMeta meta = createTestMeta();
    DatabaseLookupData data = spy(new DatabaseLookupData());

    DatabaseLookup step = createSpiedStep( db, mockHelper, meta );
    doCallRealMethod().when( step ).init( any(), any() );
    doCallRealMethod().when( step ).processRow( any(), any() );
    doCallRealMethod().when( step ).getRow();
    doCallRealMethod().when( step ).getRowHandler();
    step.setStopped( false );
    step.init( meta, data );

    data.db = db;
    data.keytypes = new int[] { ValueMetaInterface.TYPE_INTEGER };
    data.allEquals = true;
    data.conditions = new int[] { DatabaseLookupMeta.CONDITION_EQ };

    step.processRow( meta, data );

    data.lookupMeta = new RowMeta();
    data.lookupMeta.addValueMeta( new ValueMetaInteger() );

    assertNotNull( data.cache.getRowFromCache( data.lookupMeta, new Object[] { 1L } ) );
    assertNotNull( data.cache.getRowFromCache( data.lookupMeta, new Object[] { 2L } ) );
    assertNotNull( data.cache.getRowFromCache( data.lookupMeta, new Object[] { 3L } ) );
  }

  private void testIncrementLinesClusteredVariations( boolean isClustered, Boolean isExecutingClustered,
                                                      int wantedNumberOfInvocations ) {

    DatabaseLookup dbLookup = mock( DatabaseLookup.class );
    StepMeta stepMeta = mock( StepMeta.class );
    Trans trans = mock( Trans.class );

    doCallRealMethod().when( dbLookup ).incrementLines();
    doReturn( stepMeta ).when( dbLookup ).getStepMeta();
    doReturn( trans ).when( dbLookup ).getTrans();
    doReturn( isClustered ).when( stepMeta ).isClustered();
    if ( isExecutingClustered != null ) {
      doReturn( isExecutingClustered ).when( trans ).isExecutingClustered();
    }

    dbLookup.incrementLines();

    verify( dbLookup, times( wantedNumberOfInvocations ) ).incrementLinesInput();
  }

  @Test
  public void testIncrementLinesNotClustered() {
    // Not clustered, not setting running clustered
    testIncrementLinesClusteredVariations( false, null, 1 );
  }

  @Test public void testIncrementLinesClusteredNotRunningClustered() {
    // Clustered, not running clustered
    testIncrementLinesClusteredVariations( true, false, 1 );
  }

  @Test public void testIncrementLinesNotClusteredRunningClustered() {
    // Not clustered, running clustered
    testIncrementLinesClusteredVariations( false, true, 1 );
  }

  @Test
  public void testIncrementLinesClusteredRunningClustered() {
    // Clustered, running clustered
    testIncrementLinesClusteredVariations( true, true, 0 );
  }

  private void testDetermineFieldsTypesQueryingDbVariants( String kettlePropertyValue, int expectedType )
    throws KettleException {
    RowMeta rowMetaOutput = determineFieldsTypeQueryingDbSetupAndCall( kettlePropertyValue );

    assertEquals( expectedType, rowMetaOutput.getValueMeta( 1 ).getType() );
  }

  @Test
  public void determineFieldsTypesQueryingDbDefaultTest() throws KettleException {
    //Output Row Meta Must have its value be a Number instead of what is configured in the dialog, since Number
    //is the type configured in the Database
    testDetermineFieldsTypesQueryingDbVariants( null, ValueMetaInterface.TYPE_BIGNUMBER );
  }

  @Test
  public void determineFieldsTypesQueryingDbKettlePropertyFalseTest() throws KettleException {
    //Output Row Meta Must have its value be a Number instead of what is configured in the dialog, since Number
    //is the type configured in the Database
    testDetermineFieldsTypesQueryingDbVariants( "N", ValueMetaInterface.TYPE_BIGNUMBER );
  }

  @Test
  public void determineFieldsTypesQueryingDbKettlePropertyWrongValueTest() throws KettleException {
    //Output Row Meta Must have its value be a Number instead of what is configured in the dialog, since Number
    //is the type configured in the Database
    testDetermineFieldsTypesQueryingDbVariants( "somethingwrong", ValueMetaInterface.TYPE_BIGNUMBER );
  }

  @Test
  public void determineFieldsTypesQueryingDbKettlePropertyTrueTest() throws KettleException {
    //Output Row Meta Must have its value be a BigNumber instead of what is configured in the database, since BigNumber
    //is the type configured in the Dialog
    testDetermineFieldsTypesQueryingDbVariants( "Y", ValueMetaInterface.TYPE_BIGNUMBER );
  }

  private void testIsTimelessMaskNullTestVariant( String dateMask , boolean expected) {
    DatabaseLookup dbLookup = mock( DatabaseLookup.class );
    doCallRealMethod().when( dbLookup ).isTimelessMask( anyString() );
    assertEquals(expected, dbLookup.isTimelessMask( dateMask ) );
  }

  @Test
  public void isTimelessMaskNullTest() {
    testIsTimelessMaskNullTestVariant(null ,false);
  }

  @Test
  public void isTimelessMaskRandomStringTest() {
    testIsTimelessMaskNullTestVariant("randomstring" ,false );
  }

  @Test
  public void isTimelessMaskWithMaskTest() {
    testIsTimelessMaskNullTestVariant("yyyy-MM-dd hh:mm:ss" ,false );
  }

  @Test
  public void isTimelessMaskWithoutMaskTest() {
    testIsTimelessMaskNullTestVariant("yyyy-MM-dd" ,true );
  }

  private RowMeta determineFieldsTypeQueryingDbSetupAndCall( String kettlePropertyValue ) throws KettleException {
    //Row Meta - What is expected as configured in the database
    RowMeta rowMeta = new RowMeta();
    List<ValueMetaInterface> valueMetaList = new ArrayList<>();
    valueMetaList.add( new ValueMetaInteger( "int" ) );
    valueMetaList.add( new ValueMetaNumber( "num" ) );
    valueMetaList.add( new ValueMetaBigNumber( "bignum" ) );
    valueMetaList.add( new ValueMetaString( "string" ) );
    rowMeta.setValueMetaList( valueMetaList );

    //Input Row Meta
    RowMeta inputRowMeta = new RowMeta();
    List<ValueMetaInterface> inputValueMetaList = new ArrayList<>();
    inputValueMetaList.add( new ValueMetaInteger( "int" ) );
    inputRowMeta.setValueMetaList( inputValueMetaList );

    //Row Meta Output - What is expected as configured in dialog
    RowMeta rowMetaOutput = new RowMeta();
    List<ValueMetaInterface> outputValueMetaList = new ArrayList<>();
    outputValueMetaList.add( new ValueMetaInteger( "int" ) );
    outputValueMetaList.add( new ValueMetaBigNumber( "num_as_bignum" ) );
    outputValueMetaList.add( new ValueMetaBigNumber( "bignum" ) );
    rowMetaOutput.setValueMetaList( outputValueMetaList );

    //Mock Init
    DatabaseLookup dbLookup = mock( DatabaseLookup.class );
    DatabaseLookupMeta dbLookupMeta = mock( DatabaseLookupMeta.class );
    DatabaseMeta dbMeta = mock( DatabaseMeta.class );
    DatabaseLookupData dbLookupData = mock( DatabaseLookupData.class );
    Database db = mock( Database.class );
    //Expected Mock Returns
    doReturn( new String[] { "int" } ).when( dbLookupMeta ).getTableKeyField();
    doReturn( new String[] { "num", "bignum" } ).when( dbLookupMeta ).getReturnValueField();
    doReturn( dbMeta ).when( dbLookupMeta ).getDatabaseMeta();
    doReturn( "lookuptable" ).when( dbMeta ).getQuotedSchemaTableCombination( anyString(), anyString() );
    doReturn( inputRowMeta ).when( dbLookup ).getInputRowMeta();
    doReturn( rowMeta ).when( db ).getTableFields( "lookuptable" );
    //Internal State Init
    setInternalState( dbLookup, "data", dbLookupData );
    setInternalState( dbLookupData, "db", db );
    setInternalState( dbLookupData, "outputRowMeta", rowMetaOutput );
    setInternalState( dbLookup, "meta", dbLookupMeta );
    setInternalState( dbLookup, "variables", new Variables() );

    doCallRealMethod().when( dbLookup ).setVariable( anyString(), anyString() );
    doCallRealMethod().when( dbLookup ).getVariable( anyString(), anyString() );
    doCallRealMethod().when( dbLookup ).determineFieldsTypesQueryingDb();
    if ( kettlePropertyValue != null ) {
      dbLookup.setVariable( "KETTLE_COMPATIBILITY_DB_LOOKUP_USE_FIELDS_RETURN_TYPE_CHOSEN_IN_UI", kettlePropertyValue );
    }

    return rowMetaOutput;
  }

  public class MockDatabaseLookup extends DatabaseLookup {
    public MockDatabaseLookup( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
      super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    }

    @Override
    Database getDatabase( DatabaseMeta meta ) {
      try {
        return createVirtualDb( meta );
      } catch ( Exception ex ) {
        throw new RuntimeException( ex );
      }
    }
  }
}
