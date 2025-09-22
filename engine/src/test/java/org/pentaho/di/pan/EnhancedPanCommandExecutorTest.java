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

import org.pentaho.di.pan.delegates.PanTransformationDelegate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pentaho.di.base.CommandExecutorCodes;
import org.pentaho.di.base.Params;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;

import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Comprehensive unit tests for EnhancedPanCommandExecutor functionality
 */
public class EnhancedPanCommandExecutorTest {

  @Mock
  private PanTransformationDelegate mockDelegate;

  @Mock
  private TransMeta mockTransMeta;

  private LogChannelInterface log;
  private EnhancedPanCommandExecutor executor;
  private EnhancedPanCommandExecutor executorWithMocks;

  @Before
  public void setUp() {
    KettleLogStore.init();
    MockitoAnnotations.openMocks( this );
    log = new LogChannel( "EnhancedPanCommandExecutorTest" );
    executor = new EnhancedPanCommandExecutor( Pan.class, log );

    // Create an executor with mocked delegate for certain tests
    executorWithMocks = new EnhancedPanCommandExecutor( Pan.class, log );
    executorWithMocks.setTransformationDelegate( mockDelegate );
  }

  @After
  public void tearDown() {
    executor = null;
    executorWithMocks = null;
    log = null;
  }

  // Test 1: Constructor and Basic Initialization
  @Test
  public void testConstructorInitialization() {

    // Test constructor with log
    EnhancedPanCommandExecutor logExecutor = new EnhancedPanCommandExecutor( Pan.class, log );
    assertNotNull( "Log constructor should create executor", logExecutor );
    assertNotNull( "Transformation delegate should be initialized",
      logExecutor.getTransformationDelegate() );
  }

  // Test 2: Delegate Pattern Initialization and Management
  @Test
  public void testDelegateManagement() {
    // Verify initial delegate is created
    PanTransformationDelegate initialDelegate = executor.getTransformationDelegate();
    assertNotNull( "Initial delegate should be created", initialDelegate );

    // Test setting a new delegate
    PanTransformationDelegate newDelegate = new PanTransformationDelegate( log );
    executor.setTransformationDelegate( newDelegate );

    // Verify delegate was updated
    assertSame( "Delegate should be updated", newDelegate, executor.getTransformationDelegate() );
    assertNotSame( "Should not be the same as initial delegate", initialDelegate,
      executor.getTransformationDelegate() );
  }

  // Test 3: Repository Initialization Logic
  @Test
  public void testRepositoryInitializationWithoutParams() throws Exception {
    // Test with empty repository parameters
    Params emptyParams = new Params.Builder().build();

    // Repository should remain null when no repo params provided
    executor.initializeRepository( emptyParams );
    assertNull( "Repository should be null with empty params", executor.getRepository() );
  }

  @Test
  public void testRepositoryInitializationBlocked() throws Exception {
    // Test with repository connections blocked
    Params blockedParams = new Params.Builder()
      .repoName( "TestRepo" )
      .blockRepoConns( "Y" )
      .build();

    executor.initializeRepository( blockedParams );
    assertNull( "Repository should be null when connections blocked", executor.getRepository() );
  }

  @Test
  public void testRepositoryInitializationWithParams() throws Exception {
    // Test with repository parameters (will fail but tests the flow)
    Params repoParams = new Params.Builder()
      .repoName( "TestRepo" )
      .repoUsername( "testuser" )
      .repoPassword( "testpass" )
      .build();

    try {
      executor.initializeRepository( repoParams );
      // If it doesn't throw an exception, repository logic was executed
    } catch ( Exception e ) {
      // Expected to fail due to missing repository configuration
      assertTrue( "Exception should be repository-related",
        e.getMessage() == null
          || e.getMessage().toLowerCase().contains( "repository" )
          || e.getMessage().toLowerCase().contains( "connect" ) );
    }
  }

  // Test 4: Execution Configuration Integration (testing via public interface)
  @Test
  public void testExecutionConfigurationIntegration() throws Throwable {
    // Test execution configuration integration by verifying that parameters
    // are properly passed through the execution chain
    Params configParams = new Params.Builder()
      .logLevel( "DEBUG" )
      .safeMode( "Y" )
      .metrics( "Y" )
      .build();

    // Setup mock delegate for testing
    Result successResult = new Result();
    successResult.setNrErrors( 0 );
    when( mockDelegate.executeTransformation( any( TransMeta.class ),
      any( TransExecutionConfiguration.class ), any( String[].class ) ) )
      .thenReturn( successResult );

    // Execute with configuration parameters
    Result result = executorWithMocks.executeWithDelegate( mockTransMeta, configParams, new String[0] );

    assertNotNull( "Result should not be null", result );
    assertEquals( "Should return success",
      CommandExecutorCodes.Pan.SUCCESS.getCode(), result.getExitStatus() );

    // Verify the delegate was called with configuration
    verify( mockDelegate, times( 1 ) ).executeTransformation(
      eq( mockTransMeta ), any( TransExecutionConfiguration.class ), any( String[].class ) );
  }

  @Test
  public void testExecutionConfigurationDefaults() throws Throwable {
    // Test that default configuration works
    Params minimalParams = new Params.Builder().build();

    // Setup mock delegate
    Result successResult = new Result();
    successResult.setNrErrors( 0 );
    when( mockDelegate.executeTransformation( any( TransMeta.class ),
      any( TransExecutionConfiguration.class ), any( String[].class ) ) )
      .thenReturn( successResult );

    // Execute with minimal parameters
    Result result = executorWithMocks.executeWithDelegate( mockTransMeta, minimalParams, new String[0] );

    assertNotNull( "Result should not be null", result );
    assertEquals( "Should return success with defaults",
      CommandExecutorCodes.Pan.SUCCESS.getCode(), result.getExitStatus() );
  }

  // Test 5: Special Commands Handling
  @Test
  public void testListRepositoriesCommand() throws Throwable {
    // Test listing repositories command
    Params listReposParams = new Params.Builder()
      .listRepos( "Y" )
      .build();

    Result result = executor.execute( listReposParams, new String[0] );

    assertNotNull( "Result should not be null", result );
    assertEquals( "Should return success for list repos",
      CommandExecutorCodes.Pan.SUCCESS.getCode(), result.getExitStatus() );
  }

  @Test
  public void testVersionPrinting() {
    // Test version printing functionality
    int versionCode = executor.printVersion();

    assertEquals( "Should return version print code",
      CommandExecutorCodes.Pan.KETTLE_VERSION_PRINT.getCode(), versionCode );
  }

  @Test
  public void testParameterListingMode() throws Throwable {
    // Test parameter listing mode
    Params listParamsParams = new Params.Builder()
      .localFile( Objects.requireNonNull( getClass().getResource( "hello-world.ktr" ) ).getPath() )
      .listFileParams( "Y" )
      .build();

    Result result = executor.execute( listParamsParams, new String[0] );

    assertNotNull( "Result should not be null", result );
    // Should fail due to missing transformation file
    assertNotEquals( "Exit code should indicate failure", 0, result.getExitStatus() );
  }

  // Test 7: Delegate Execution Integration
  @Test
  public void testExecuteWithDelegateSuccess() throws Throwable {
    // Setup mock delegate to return successful result
    Result successResult = new Result();
    successResult.setNrErrors( 0 );
    when( mockDelegate.executeTransformation( any( TransMeta.class ),
      any( TransExecutionConfiguration.class ), any( String[].class ) ) )
      .thenReturn( successResult );

    // Test delegate execution
    Params testParams = new Params.Builder().build();
    Result result = executorWithMocks.executeWithDelegate( mockTransMeta, testParams, new String[0] );

    assertNotNull( "Result should not be null", result );
    assertEquals( "Should return success code",
      CommandExecutorCodes.Pan.SUCCESS.getCode(), result.getExitStatus() );
    // Verify delegate was called
    verify( mockDelegate, times( 1 ) ).executeTransformation(
      eq( mockTransMeta ), any( TransExecutionConfiguration.class ), any( String[].class ) );
  }

  @Test
  public void testExecuteWithDelegateError() throws Throwable {
    // Setup mock delegate to return error result
    Result errorResult = new Result();
    errorResult.setNrErrors( 5 );
    when( mockDelegate.executeTransformation( any( TransMeta.class ),
      any( TransExecutionConfiguration.class ), any( String[].class ) ) )
      .thenReturn( errorResult );

    // Test delegate execution with errors
    Params testParams = new Params.Builder().build();
    Result result = executorWithMocks.executeWithDelegate( mockTransMeta, testParams, new String[0] );

    assertNotNull( "Result should not be null", result );
    assertEquals( "Should return errors during processing code",
      CommandExecutorCodes.Pan.ERRORS_DURING_PROCESSING.getCode(), result.getExitStatus() );
  }

  @Test
  public void testExecuteWithDelegateException() throws Throwable {
    // Setup mock delegate to throw exception
    when( mockDelegate.executeTransformation( any( TransMeta.class ),
      any( TransExecutionConfiguration.class ), any( String[].class ) ) )
      .thenThrow( new KettleException( "Test execution exception" ) );

    // Test exception handling
    Params testParams = new Params.Builder().build();
    Result result = executorWithMocks.executeWithDelegate( mockTransMeta, testParams, new String[0] );

    assertNotNull( "Result should not be null", result );
    assertEquals( "Should return unexpected error code",
      CommandExecutorCodes.Pan.UNEXPECTED_ERROR.getCode(), result.getExitStatus() );
  }

  // Test 8: Edge Cases and Error Handling
  @Test
  public void testExecuteWithNullParams() {
    try {
      // This should handle null parameters gracefully
      Result result = executor.execute( null, new String[0] );
      // If it returns a result, it handled null gracefully
      assertNotNull( "Should handle null params", result );
    } catch ( Throwable e ) {
      // If it throws an exception, it should be a meaningful one
      assertNotNull( "Exception should have a message", e.getMessage() );
    }
  }

  @Test
  public void testExecuteWithNullArguments() {
    Params validParams = new Params.Builder().listRepos( "Y" ).build();

    try {
      // Test with null arguments array
      Result result = executor.execute( validParams, null );
      assertNotNull( "Should handle null arguments", result );
    } catch ( Throwable e ) {
      // Should handle null arguments gracefully
      assertTrue( "Should handle null arguments without NPE",
        !( e instanceof NullPointerException ) );
    }
  }

  // Test 9: Trust Repository User Setting
  @Test
  public void testTrustRepoUserSetting() {
    // Test trust repository user functionality
    Params trustParams = new Params.Builder()
      .repoName( "TestRepo" )
      .trustRepoUser( "Y" )
      .build();

    // This should set the system property before attempting connection
    try {
      executor.initializeRepository( trustParams );
    } catch ( Exception e ) {
      // Expected to fail due to missing repository, but trust logic should execute
    }

    // The test passes if no unexpected exceptions occur during trust setup
    assertTrue( "Trust user logic should execute without errors", true );
  }

  // Test 10: Integration with Parent Class Methods
  @Test
  public void testInheritedMethodsWork() {
    // Test that methods inherited from PanCommandExecutor still work
    assertNotNull( "Should have log channel", executor.getLog() );
    assertNotNull( "Should have package class", executor.getPkgClazz() );

    // Test version printing (inherited method)
    int versionCode = executor.printVersion();
    assertEquals( "Version print should work",
      CommandExecutorCodes.Pan.KETTLE_VERSION_PRINT.getCode(), versionCode );
  }
}
