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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import mondrian.olap.MondrianProperties;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Test;
import org.olap4j.OlapConnection;
import org.olap4j.OlapStatement;

public class ConcurrencySupportTest {

  private static final int THREAD_POOL_SIZE = 10;

  private static final int QUERIES_COUNT = 100;

  private static final int TIMEOUT = 20;

  private static final String SQL_QUERY = "select time_by_day.the_year as c0, customer.gender as c1,\n"
      + "count(sales_fact_1997.product_id) as m0\n"
      + "from\n" 
      + "time_by_day time_by_day,\n"
      + "sales_fact_1997 sales_fact_1997,\n"
      + "customer customer\n" 
      + "where\n"
      + "sales_fact_1997.time_id = time_by_day.time_id\n"
      + "and\n" 
      + "time_by_day.the_year = 1997\n" 
      + "and\n"
      + "sales_fact_1997.customer_id = customer.customer_id\n" 
      + "and\n" 
      + "customer.gender = 'M'\n"
      + "group by\n"
      + "time_by_day.the_year,\n"
      + "customer.gender\n" 
      + "union all\n" 
      + "select\n"
      + "time_by_day.the_year as c0,\n"
      + "customer.gender as c1,\n" 
      + "count(sales_fact_1997.product_id) as m0\n"
      + "from\n"
      + "time_by_day time_by_day,\n" 
      + "sales_fact_1997 sales_fact_1997,\n" 
      + "customer customer\n" + "where\n"
      + "sales_fact_1997.time_id = time_by_day.time_id\n"
      + "and\n" 
      + "time_by_day.the_year = 1997\n" 
      + "and\n"
      + "sales_fact_1997.customer_id = customer.customer_id\n"
      + "and\n" + "customer.gender = 'M'\n" + "group by\n"
      + "time_by_day.the_year,\n"
      + "customer.gender";

  private static final String MDX_QUERY = "select {[Measures].[Sales Count], "
      + "[Measures].[Store Invoice]} on 0, "
      + "{([Gender].[M],[Time].[1997])} on 1 "
      + "from [Warehouse and Sales]";
  
  private volatile boolean operationStartedFlag = false;
  
  @Test
  public void testQueryCancellationWithoutDBCPPool() throws Exception {
    final TestConnectionFactory connectionFactory =
        ConnectionFactoryRegistry.getDefaultDriverManagerConnectionFactory();
    try {
      connectionFactory.createConnection();
    } catch ( Throwable e ) {
      fail( "Exception occurred while trying to get a connection.\n" + ExceptionUtils.getStackTrace( e ) );
    }
    testQueryCancellation( connectionFactory );
  }
  
  @Test
  public void testQueryCancellationWithDBCPPool() throws Exception {
    final TestConnectionFactory connectionFactory = ConnectionFactoryRegistry.getDefaultDBCPConnectionFactory();
    try {
      connectionFactory.createConnection();
    } catch ( Throwable e ) {
      fail( "Exception occurred while trying to get a connection.\n" + ExceptionUtils.getStackTrace( e ) );
    }
    testQueryCancellation( connectionFactory );
  }
  
  @Test
  public void testMDXQueryCancellationWithoutDBCPPool() throws Exception {
    final TestConnectionFactory connectionFactory =
        ConnectionFactoryRegistry.getDriverManagerConnectionFactory( MondrianProperties.instance().TestConnectString
            .get() );
    try {
      Connection connection = connectionFactory.createConnection();
      connection.unwrap( OlapConnection.class );
    } catch ( Throwable e ) {
      fail( "Exception occurred while trying to get an olap connection.\n" + ExceptionUtils.getStackTrace( e ) );
    }
    testMDXQueryCancellation( connectionFactory );

  }

  @Test
  public void testMDXQueryCancellationWithDBCPPool() throws Exception {
    final TestConnectionFactory connectionFactory = ConnectionFactoryRegistry.getDBCPConnectionFactory(MondrianProperties.instance().TestConnectString.get());
    try {
      Connection connection = connectionFactory.createConnection();
      connection.unwrap( OlapConnection.class );
    } catch (Throwable e) {
      fail("Exception occurred while trying to get an olap connection.\n" + ExceptionUtils.getStackTrace( e ));
    }
    testMDXQueryCancellation( connectionFactory );

  }
  
  private void testQueryCancellation(final TestConnectionFactory connectionFactory) throws InterruptedException {
    final List<Statement> statements = new ArrayList<>();
    operationStartedFlag = false;
    ExecutorService executorService = Executors.newFixedThreadPool( THREAD_POOL_SIZE );
    for ( int i = 0; i < QUERIES_COUNT; i++ ) {
      executorService.submit( new Runnable() {
        @Override
        public void run() {
          Statement statement = null;
          try {
            Connection jdbcConnection = connectionFactory.createConnection();
            statement = jdbcConnection.createStatement();
            statements.add( statement );
            statement.execute( SQL_QUERY );
          } catch ( SQLException e ) {
            System.err.println( "Exception occurred while trying to execute query.\n" + ExceptionUtils.getStackTrace( e ) );
          } finally {
            close( statement );
            operationStartedFlag = true;
          }
        }
      } );
    }

    randomlyCancel(statements, executorService);
  }
  
  private void testMDXQueryCancellation(final TestConnectionFactory connectionFactory) throws InterruptedException {
    final List<Statement> statements = new ArrayList<>();
    operationStartedFlag = false;
    ExecutorService executorService = Executors.newFixedThreadPool( THREAD_POOL_SIZE );
    for ( int i = 0; i < QUERIES_COUNT; i++ ) {
      executorService.submit( new Runnable() {
        @Override
        public void run() {
          OlapConnection olapConnection = null;
          OlapStatement statement = null;
          try {
            Connection connection = connectionFactory.createConnection();
            olapConnection = connection.unwrap( OlapConnection.class );
            statement = olapConnection.createStatement();
            
            statements.add( statement );
            statement.executeOlapQuery( MDX_QUERY );
          } catch ( SQLException e ) {
            System.err.println( "Exception occurred while trying to execute MDX query.\n" + ExceptionUtils.getStackTrace( e ) );
          } finally {
            close( statement );
            operationStartedFlag = true;
          }
        }
      } );
    }
    
    randomlyCancel(statements, executorService);
  }

  private void randomlyCancel( List<Statement> statements, ExecutorService executorService ) throws InterruptedException {
    final Random random = new Random();
    int tryToCancel = 0;
    try {
      while ( !operationStartedFlag ) {
        Thread.sleep( 100 );
      }
      for ( int i = 0; i < 20; i++ ) {
        try {
          if ( statements.size() > 0 ) {
            Statement statement = statements.get( random.nextInt( statements.size() ) );
            if ( !statement.isClosed() ) {
              tryToCancel++;
              statement.cancel();
            }
          }
        } catch ( SQLException e ) {
            shutdownExecutorService(executorService);
            fail( "Exception occurred while trying to cancel query.\n" + ExceptionUtils.getStackTrace( e ) );
        }
      }
    } catch ( InterruptedException e ) {
      shutdownExecutorService(executorService);
      fail("Unexpected interrupt");
    }
    shutdownExecutorService(executorService);
    assertThat( tryToCancel, is(  not( 0 ) ) );
  }
  
  private static void shutdownExecutorService(ExecutorService executorService) throws InterruptedException {
    executorService.shutdown();
    executorService.awaitTermination( TIMEOUT, TimeUnit.SECONDS );
  }

  private static void close( Statement statement ) {
    if ( statement != null ) {
      try {
        if ( !statement.isClosed() ) {
          statement.close();
        }
      } catch ( SQLException e ) {
        System.err.println(  "Exception occurred while trying to close statement.\n" + ExceptionUtils.getStackTrace( e )  );
      }
    }
  }

}
