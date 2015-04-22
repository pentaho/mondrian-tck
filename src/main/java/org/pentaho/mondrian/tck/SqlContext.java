/*******************************************************************************
 *
 * Pentaho Mondrian Test Compatibility Kit
 *
 * Copyright (C) 2013-2014 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.pentaho.mondrian.tck;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.rolap.RolapUtil;
import mondrian.spi.Dialect;
import mondrian.spi.DialectManager;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;
import org.junit.Assert;

public class SqlContext extends Context {

  private static final Logger LOGGER = Logger.getLogger( SqlContext.class );
  private static final DataSource ds;
  private static final ScheduledExecutorService executor =
      Util.getScheduledExecutorService(10, "SqlContext.canceler" );

  Connection connection;
  private final AtomicBoolean isStale = new AtomicBoolean( false );

  static {
    // Load the drivers
    RolapUtil.loadDrivers( MondrianProperties.instance().JdbcDrivers.get() );

    // Create a BDS instance
    BasicDataSource bds = new BasicDataSource();

    // Configure it
    bds.setUrl( MondrianProperties.instance().FoodmartJdbcURL.get() );
    bds.setUsername( MondrianProperties.instance().TestJdbcUser.get() );
    bds.setPassword( MondrianProperties.instance().TestJdbcPassword.get() );

    // Set the pool parameters.
    // We need the pool to keep alive the connections between the runs so
    // we can test the concurrency compliance of the driver.
    bds.setMaxActive( 5 );
    bds.setMinIdle( 5 );

    ds = bds;
  }

  private SqlContext( final Connection connection ) {
    this.connection = connection;
  }

  public static SqlContext forConnection( String connectionString ) throws Exception {
    // Load the drivers
    RolapUtil.loadDrivers( MondrianProperties.instance().JdbcDrivers.get() );
    Connection connection =
        DriverManager.getConnection(
          connectionString,
          MondrianProperties.instance().TestJdbcUser.get(),
          MondrianProperties.instance().TestJdbcPassword.get() );
    return new SqlContext( connection );
  }

  public static SqlContext defaultContext() throws Exception {
    return forConnection( MondrianProperties.instance().FoodmartJdbcURL.get() );
  }

  public static SqlContext dbcpContext() throws Exception {
    return new SqlContext( ds.getConnection() );
  }

  public void verify( SqlExpectation expectation ) throws Exception {
    if ( isStale.get() ) {
      throw new RuntimeException( "Stale SqlContext detected." );
    }

    try ( final Statement statement = connection.createStatement() ) {

      long startTime = System.currentTimeMillis();
      if ( expectation.cancelTimeout >= 0 && Math.random() > 0.5 ) {
        // This means that this query must be interrupted in N seconds.
        executor.schedule(
            new Callable<Void>() {
              public Void call() throws Exception {
                try {
                  LOGGER.info( "Canceling SQL query" );
                  statement.cancel();
                  LOGGER.info( "Canceling suceeded" );
                } catch ( Throwable t ) {
                  LOGGER.error( "Error while canceling query.", t );
                }
                return null;
              }
            },
            expectation.cancelTimeout,
            TimeUnit.MILLISECONDS );
      }
      final ResultSet rs = expectation.query.getData( connection, statement );

      if ( expectation.cancelTimeout >= 0 ) {
        if ( System.currentTimeMillis() - startTime < expectation.cancelTimeout ) {
          Assert.fail( "Query canceled after it finished." );
        }
        Assert.fail( "Query should have been canceled." );
      }

      expectation.verify( rs );

    } finally {
      dispose();
    }
  }

  public void dispose() throws Exception {
    isStale.set( true );
    connection.close();
  }

  public Dialect getDialect() {
    return DialectManager.createDialect( null, connection );
  }

  public static int getSqlComplianceLevel() {
    return Integer.valueOf(
      testProperties.getProperty( "sql.compliance.level" ) );
  }
}
