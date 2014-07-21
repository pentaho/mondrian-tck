package org.pentaho.mondrian.tck;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;

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
        RolapUtil.loadDrivers( MondrianProperties.instance().JdbcDrivers.get() );
        Connection connection = DriverManager.getConnection( key, "foodmart", "foodmart" );
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
    statement.execute( expectation.query );
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
}
