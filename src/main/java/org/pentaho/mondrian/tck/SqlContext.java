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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Function;
import mondrian.olap.MondrianProperties;
import mondrian.rolap.RolapUtil;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class SqlContext extends Context {

  private static final LoadingCache<String, SqlContext> instances =
    CacheBuilder.newBuilder().build(new CacheLoader<String, SqlContext>() {
      @Override
      public SqlContext load( String key ) throws Exception {
        // Load the drivers
        RolapUtil.loadDrivers( MondrianProperties.instance().JdbcDrivers.get() );
        Connection connection =
          DriverManager.getConnection(
            key,
            MondrianProperties.instance().TestJdbcUser.get(),
            MondrianProperties.instance().TestJdbcPassword.get() );
        return new SqlContext( connection );
      }
    } );

  private Connection connection;

  private SqlContext( final Connection connection ) {
    this.connection = connection;
  }

  public static SqlContext forConnection( String connectionString ) throws ExecutionException, IOException {
    return instances.get( connectionString );
  }

  public static SqlContext defaultContext() throws IOException, ExecutionException {
    return forConnection( MondrianProperties.instance().FoodmartJdbcURL.get() );
  }

  public void verify( SqlExpectation expectation ) throws Exception {
    final Statement statement = connection.createStatement();
    for ( Function<Statement, Void> statementModifier : expectation.statementModifiers ) {
      statementModifier.apply( statement );
    }
    try {
      statement.execute( expectation.query );
    } catch ( Throwable t ) {
      throw new Exception( "Query failed to run successfully.", t );
    }

    final ResultSet rs = statement.getResultSet();

    try {
      expectation.verify( rs );
    } finally {
      try {
        rs.close();
      } catch ( Exception e ) {
        // no op.
        e.printStackTrace();
      }
      try {
        statement.close();
      } catch ( Exception e ) {
        // no op.
        e.printStackTrace();
      }
    }
  }
  
  public void executeJDBCCode(IJDBCExecutable exec)throws Exception{
    exec.executeCode( connection );
  }
  
  public interface IJDBCExecutable{
    public void executeCode( Connection conn) throws Exception;
  }
}
