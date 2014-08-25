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

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import mondrian.olap.MondrianProperties;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Test;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapStatement;

public class ConcurrencySupportTest {

  private static final int POOL_SIZE_10 = 10;
  
  private static final int POOL_SIZE_5 = 5;

  private static final int POOL_SIZE_1 = 1;
  
  private static final int THREAD_SIZE_10 = 10;
  
  private static final int QUERY_SIZE_20 = 20;
  
  private static final StringQuery[] SQLQUERYS =new StringQuery[]{
    new StringQuery( "SELECT  sales_fact_1998.store_sales,  sales_fact_1998.store_cost,"
        + "sales_fact_1998.unit_sales FROM public.sales_fact_1998 WHERE"
        + "sales_fact_1998.store_sales > sales_fact_1998.unit_sales;" ),
    new StringQuery( "SELECT * FROM public.sales_fact_1998 WHERE sales_fact_1998.store_sales > 0"
        + " and  sales_fact_1998.unit_sales <1000 ORDER BY sales_fact_1998.store_cost DESC;" ),
    new StringQuery( "SELECT * FROM public.sales_fact_1998 WHERE sales_fact_1998.unit_sales <500"
        +" ORDER BY sales_fact_1998.store_sales DESC;" ),
    new StringQuery( "SELECT * FROM public.sales_fact_1998 ORDER BY sales_fact_1998.store_cost DESC;" ),
    new StringQuery( "SELECT * FROM public.sales_fact_1998 WHERE sales_fact_1998.store_sales > 3" )
    };

  private static final StringQuery[] MDXQUERYS = new StringQuery[]{
    new StringQuery("select {[Measures].[Sales Count], " 
        + "[Measures].[Store Invoice]} on 0, "
        + "{([Gender].[M],[Time].[1997])} on 1 " 
        + "from [Warehouse and Sales]"),
    new StringQuery( "select {[Measures].[Sales Count], "
        + "[Measures].[Store Invoice]} on 0, "
        + "{[Time].[1997],[Time].[1998]} on 1 "
        + "from [Warehouse and Sales]" ),
    new StringQuery( "select {[Measures].[Sales Count], "
        + "[Measures].[Store Invoice]} on 0, "
        + "{([Gender].[F],[Time].[1997])} on 1 "
        + "from [Warehouse and Sales]" ),
    new StringQuery( "select {[Measures].[Store Cost], "
        + "[Measures].[Supply Time]} on 0 from [Warehouse and Sales]" ),
    new StringQuery( "select {[Measures].[Store Sales], "
        + "[Measures].[Units Ordered]} on 0 from [Warehouse and Sales]" ),
    new StringQuery( "select {[Measures].[Unit Sales], "
        + "[Measures].[Units Ordered]} on 0 from [Warehouse and Sales]" ),
    new StringQuery(  "select {[Measures].[Profit], "
        + "[Measures].[Units Shipped]} on 0 from [Warehouse and Sales]" ),
    new StringQuery("select {[Measures].[Unit Sales]} on columns from Sales" ),
    new StringQuery("select {[Measures].[Store Cost]} on 0 from [Sales] "),
    new StringQuery("select {[Measures].[Store Cost]} on 0 from [Sales] ")
  };
  
  private static final class StringQuery {
    final String query;

    public StringQuery(String query) {
        this.query = query;
    }
}

  @Test
  public void testQueryCancellationSingleConnection() throws Exception {
    try {
      String url = MondrianProperties.instance().FoodmartJdbcURL.get();
      TestConnectionFactory connectionFactory = ConnectionFactoryRegistry.getDriverManagerConnectionFactory( url );
      testQueryCancellation( connectionFactory, false );
    } catch ( Exception e ) {
      fail( "Exception occurred while thread executing" + ExceptionUtils.getStackTrace( e ) );
    }
  }

  @Test
  public void testQueryCancellationPoolSize_1() throws Exception {
    try {
      String url = MondrianProperties.instance().FoodmartJdbcURL.get();
      TestConnectionFactory connectionFactory = ConnectionFactoryRegistry.getDBCPConnectionFactory( url, POOL_SIZE_1 );
      testQueryCancellation( connectionFactory, true );
    } catch ( Exception e ) {
      fail( "Exception occurred while thread executing" + ExceptionUtils.getStackTrace( e ) );
    }
  }
  
  @Test
  public void testQueryCancellationPoolSize_10() throws Exception {
    try {
      String url = MondrianProperties.instance().FoodmartJdbcURL.get();
      TestConnectionFactory connectionFactory = ConnectionFactoryRegistry.getDBCPConnectionFactory( url, POOL_SIZE_10 );
      testQueryCancellation( connectionFactory, true );
    } catch ( Exception e ) {
      fail( "Exception occurred while thread executing" + ExceptionUtils.getStackTrace( e ) );
    }
  }
  
  @Test
  public void testQueryCancellationMultiThread10AndPoolSize5() throws Exception {
    try {
      String url = MondrianProperties.instance().FoodmartJdbcURL.get();
      TestConnectionFactory connectionFactory = ConnectionFactoryRegistry.getDBCPConnectionFactory( url, POOL_SIZE_5 );
      testQueryCancellationMultiThread( connectionFactory,  THREAD_SIZE_10);
    } catch ( Exception e ) {
      fail( "Exception occurred while thread executing" + ExceptionUtils.getStackTrace( e ) );
    }
  }
  
  @Test
  public void testMDXCancellationSingleConnection() throws Exception {
    try {
      String url = MondrianProperties.instance().TestConnectString.get();
      TestConnectionFactory connectionFactory = ConnectionFactoryRegistry.getDriverManagerConnectionFactory( url );
      testMDXCancellation( connectionFactory, false );
    } catch ( Exception e ) {
      fail( "Exception occurred while thread executing" + ExceptionUtils.getStackTrace( e ) );
    }
  }

  @Test
  public void testMDXCancellationPoolSize_1() throws Exception {
    try {
      String url = MondrianProperties.instance().TestConnectString.get();
      TestConnectionFactory connectionFactory = ConnectionFactoryRegistry.getDBCPConnectionFactory( url, POOL_SIZE_1 );
      testMDXCancellation( connectionFactory, true );
    } catch ( Exception e ) {
      fail( "Exception occurred while thread executing" + ExceptionUtils.getStackTrace( e ) );
    }
  }
  
  @Test
  public void testMDXCancellationPoolSize_10() throws Exception {
    try {
      String url = MondrianProperties.instance().TestConnectString.get();
      TestConnectionFactory connectionFactory = ConnectionFactoryRegistry.getDBCPConnectionFactory( url, POOL_SIZE_10 );
      testMDXCancellation( connectionFactory, true );
    } catch ( Exception e ) {
      fail( "Exception occurred while thread executing" + ExceptionUtils.getStackTrace( e ) );
    }
  }
  
  @Test
  public void testMDXCancellationMultiThread10AndPoolSize5() throws Exception {
    try {
      String url = MondrianProperties.instance().TestConnectString.get();
      TestConnectionFactory connectionFactory = ConnectionFactoryRegistry.getDBCPConnectionFactory( url, POOL_SIZE_5 );
      testMDXCancellationMultiThread( connectionFactory, THREAD_SIZE_10 );
    } catch ( Exception e ) {
      fail( "Exception occurred while thread executing" + ExceptionUtils.getStackTrace( e ) );
    }
  }

  private void testQueryCancellation( final TestConnectionFactory connectionFactory, boolean isOneConnectionOneStatement  ) {
    try {
      Connection connection = connectionFactory.getConnection();
      final Statement statement = connection.createStatement();
      final List<Throwable> exceptions = Collections.synchronizedList( new ArrayList<Throwable>() );
      final Random random = new Random();
      // start execute in another tread
      new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              statement.execute( SQLQUERYS[random.nextInt( SQLQUERYS.length )].query );
            } catch ( SQLException e ) {
              exceptions.add( e );
            }
          }
        } ).start();
      // wait
      Thread.sleep( 10 );
      assertTrue( "Failed! Statement is closed!", !statement.isClosed() );
      statement.cancel();
      statement.close();
      assertTrue( "Failed with exception(s): " + exceptions, exceptions.isEmpty() );
      // wait
      Thread.sleep( 10 );
      if (isOneConnectionOneStatement){
        connection.close();
        connection = connectionFactory.getConnection();
      }
      Statement secondStatement = connection.createStatement();
      assertTrue( secondStatement.execute( SQLQUERYS[random.nextInt( SQLQUERYS.length )].query ) );
      secondStatement.close();
      connection.close();
    } catch ( Exception e ) {
      fail( "Exception occurred while thread executing" + ExceptionUtils.getStackTrace( e ) );
    }
  }
  
  private void testQueryCancellationMultiThread( final TestConnectionFactory connectionFactory, int threadPoolSize ) {
    try { 
      final List<Statement> statements = Collections.synchronizedList( new ArrayList<Statement>() );
      final List<Throwable> exceptions = Collections.synchronizedList( new ArrayList<Throwable>() );
      final Random random = new Random();
      ExecutorService poolExecutor = Executors.newFixedThreadPool( threadPoolSize );
      // start execute query in another treads
      for ( int i = 0; i < QUERY_SIZE_20; i++ ) {       
      poolExecutor.submit( new Runnable() {
          @Override
          public void run() {
            Connection connection = null;
            try {
              connection = connectionFactory.getConnection();
              Statement statement = connection.createStatement();
              statements.add( statement );
              statement.execute( SQLQUERYS[random.nextInt( SQLQUERYS.length )].query );
            } catch ( SQLException e ) {
              exceptions.add( e );
            } finally {
              if (connection != null){
                try {
                  connection.close();
                } catch ( SQLException e ) {
                  exceptions.add( e );
                }
              }
            }
          }
        });
      }
      // stop execute query in another treads
      for ( int i = 0; i < QUERY_SIZE_20; i++ ) {     
        if (statements.size()>0){
          Statement s = statements.get( random.nextInt(statements.size()));
          if ( !s.isClosed() ) {
               s.cancel();
               s.close();
          }
        }
      }
      assertTrue( "Failed with exception(s): " + exceptions, exceptions.isEmpty() );
    } catch ( Exception e ) {
      fail( "Exception occurred while thread executing" + ExceptionUtils.getStackTrace( e ) );
    }
  }

  private void testMDXCancellation( final TestConnectionFactory connectionFactory, boolean isOneConnectionOneStatement ) {
    try {
      Connection connection = connectionFactory.getConnection();
      OlapConnection olapConnection = connection.unwrap( OlapConnection.class );
      final OlapStatement statement = olapConnection.createStatement();
      final List<Throwable> exceptions = Collections.synchronizedList( new ArrayList<Throwable>() );
      final Random random = new Random();
      // start execute in another tread
      new Thread( new Runnable() {
        @Override
        public void run() {
          try {
            statement.executeOlapQuery( MDXQUERYS[random.nextInt( MDXQUERYS.length )].query );
          } catch ( SQLException e ) {
            exceptions.add( e );
          }
        }
      } ).start();
      // wait
      Thread.sleep( 10 );
      assertTrue( "Failed! Statement is closed!", !statement.isClosed() );
      statement.cancel();
      statement.close();
      assertTrue( "Failed with exception(s): " + exceptions, exceptions.isEmpty() );
      // wait
      Thread.sleep( 10 );
      if (isOneConnectionOneStatement){
        connection.close();
        connection = connectionFactory.getConnection();
        olapConnection = connection.unwrap( OlapConnection.class );
      }
      OlapStatement secondStatement = olapConnection.createStatement();
      CellSet cellSet = secondStatement.executeOlapQuery( MDXQUERYS[random.nextInt( MDXQUERYS.length )].query );
      assertNotNull( cellSet );
      secondStatement.close();
      olapConnection.close();
    } catch ( Exception e ) {
      fail( "Exception occurred while thread executing" + ExceptionUtils.getStackTrace( e ) );
    }
  }
  
  private void testMDXCancellationMultiThread( final TestConnectionFactory connectionFactory, int threadPoolSize ) {
    try { 
      final List<Statement> statements = Collections.synchronizedList( new ArrayList<Statement>() );
      final List<Throwable> exceptions = Collections.synchronizedList( new ArrayList<Throwable>() );
      final Random random = new Random();
      ExecutorService poolExecutor = Executors.newFixedThreadPool( threadPoolSize );
      // start execute query in another treads
      for ( int i = 0; i < QUERY_SIZE_20; i++ ) {       
      poolExecutor.submit( new Runnable() {
          @Override
          public void run() {
            Connection connection = null;
            try {
              connection = connectionFactory.getConnection();
              OlapConnection olapConnection = connection.unwrap( OlapConnection.class );
              OlapStatement statement = olapConnection.createStatement();
              statements.add( statement );
              statement.executeOlapQuery( MDXQUERYS[random.nextInt( MDXQUERYS.length )].query );
            } catch ( SQLException e ) {
              exceptions.add( e );
            } finally {
              if (connection != null){
                try {
                  connection.close();
                } catch ( SQLException e ) {
                  exceptions.add( e );
                }
              }
            }
          }
        });
      }
      // stop execute query in another treads
      for ( int i = 0; i < QUERY_SIZE_20; i++ ) {     
        if (statements.size()>0){
          Statement s = statements.get( random.nextInt(statements.size()));
          if ( !s.isClosed() ) {
               s.cancel();
               s.close();
          }
        }
      }
      assertTrue( "Failed with exception(s): " + exceptions, exceptions.isEmpty() );
    } catch ( Exception e ) {
      fail( "Exception occurred while thread executing" + ExceptionUtils.getStackTrace( e ) );
    }
  }
}
