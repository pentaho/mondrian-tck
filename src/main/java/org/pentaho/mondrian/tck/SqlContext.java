/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */
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

  private Connection connection;
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
      }

      expectation.verify( rs );

      Assert.fail( "Query should have been canceled." );

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
}
