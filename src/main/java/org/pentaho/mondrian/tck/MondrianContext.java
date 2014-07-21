package org.pentaho.mondrian.tck;


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import mondrian.olap.MondrianProperties;
import mondrian.rolap.RolapUtil;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.OlapStatement;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MondrianContext {

  public static final Properties testProperties;

  static {
    try {
      testProperties = loadTestProperties();
      if ( Boolean.parseBoolean( testProperties.getProperty( "register.big-data-plugin" ) ) ) {
        BigDataPluginUtil.prepareBigDataPlugin(
          new File( testProperties.getProperty( "big-data-plugin.folder" ) ),
          testProperties.getProperty( "active.hadoop.configuration" ) );
      }
    } catch ( Exception e ) {
      throw new RuntimeException( e );
    }
  }

  private static Properties loadTestProperties() throws IOException {
    Properties testProperties = new Properties();
    testProperties.load( new BufferedReader( new FileReader( "test.properties" ) ) );
    return testProperties;
  }

  private static final LoadingCache<String, MondrianContext> instances =
    CacheBuilder.newBuilder().build(new CacheLoader<String, MondrianContext>() {
      @Override
      public MondrianContext load( String key ) throws Exception {
        Connection connection = DriverManager.getConnection( key );
        OlapConnection olapConnection = connection.unwrap( OlapConnection.class );
        return new MondrianContext( olapConnection );
      }
    } );

  private OlapConnection olapConnection;

  private MondrianContext( final OlapConnection olapConnection ) {
    this.olapConnection = olapConnection;
  }

  public static MondrianContext forConnection( String connectionString ) throws ExecutionException, IOException {
    return instances.get( connectionString );
  }

  public static MondrianContext defaultContext() throws IOException, ExecutionException {
    return forConnection( MondrianProperties.instance().TestConnectString.get() );
  }

  public void verify( MondrianExpectation expectation ) throws OlapException {
    final List<String> sqls = new ArrayList<>();
    RolapUtil.ExecuteQueryHook existingHook = RolapUtil.getHook();
    RolapUtil.setHook( sqlCollector( sqls ) );
    OlapStatement statement = olapConnection.createStatement();
    CellSet cellSet = statement.executeOlapQuery( expectation.getQuery() );
    RolapUtil.setHook( existingHook );
    expectation.verify( cellSet, sqls );
  }

  private RolapUtil.ExecuteQueryHook sqlCollector( final List<String> sqls ) {
    return new RolapUtil.ExecuteQueryHook() {
      @Override
      public void onExecuteQuery( String sql ) {
        sqls.add( sql );
      }
    };
  }
}
