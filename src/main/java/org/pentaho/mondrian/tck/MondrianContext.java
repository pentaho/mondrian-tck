package org.pentaho.mondrian.tck;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import mondrian.olap.MondrianProperties;
import mondrian.rolap.RolapUtil;

import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.OlapStatement;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class MondrianContext extends Context {

  private static final LoadingCache<String, MondrianContext> instances =
    CacheBuilder.newBuilder().build( new CacheLoader<String, MondrianContext>() {
      @Override
      public MondrianContext load( String key ) throws Exception {
        Connection connection = DriverManager.getConnection( key );
        OlapConnection olapConnection = connection.unwrap( OlapConnection.class );
        return new MondrianContext( olapConnection, key );
      }
    } );

  private static final LoadingCache<String, File> catalogs =
    CacheBuilder.newBuilder().build( new CacheLoader<String, File>() {
      @Override
      public File load( String key ) throws Exception {
        File catalogFile = File.createTempFile( "temp", ".xml" );
        catalogFile.deleteOnExit();
        Writer writer = new BufferedWriter( new FileWriter( catalogFile ) );
        writer.write( key );
        writer.close();
        return catalogFile;
      }
    } );

  private OlapConnection olapConnection;
  private String connectString;

  private MondrianContext( final OlapConnection olapConnection, final String connectString ) {
    this.olapConnection = olapConnection;
    this.connectString = connectString;
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

  public MondrianContext withCatalog( String catalogXml ) throws IOException, ExecutionException {
    File catalogFile = catalogs.get( catalogXml );
    return forConnection( replaceCatalog( connectString, catalogFile ) );
  }

  private String replaceCatalog( final String connectString, final File catalogFile ) {
    return connectString.replaceFirst( "Catalog=[^;]+;", "Catalog=" + catalogFile.getAbsolutePath() + ";" );
  }
}
