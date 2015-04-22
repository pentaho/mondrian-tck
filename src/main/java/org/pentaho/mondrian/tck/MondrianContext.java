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

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.rolap.RolapConnection;
import mondrian.rolap.RolapUtil;

import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
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
          return new MondrianContext( olapConnection );
        }
      } );

  private static final LoadingCache<String, Path> catalogs =
      CacheBuilder.newBuilder().build( new CacheLoader<String, Path>() {
        @Override
        public Path load( String key ) throws Exception {
          Path catalogFile = Files.createTempFile( "temp", ".xml" );
          catalogFile.toFile().deleteOnExit();
          try ( Writer writer = Files.newBufferedWriter( catalogFile, Charset.defaultCharset() ) ) {
            writer.write( key );
          }
          return catalogFile;
        }
      } );

  OlapConnection olapConnection;

  private MondrianContext( final OlapConnection olapConnection ) {
    this.olapConnection = olapConnection;
  }

  public static MondrianContext forConnection( String connectionString ) throws ExecutionException, IOException {
    return instances.get( connectionString );
  }

  public static MondrianContext forCatalog( String catalog ) throws IOException, ExecutionException {
    return forConnection(
        replaceCatalog( MondrianProperties.instance().TestConnectString.get(), catalogs.get( catalog ) ) );
  }

  public static MondrianContext forCatalog( String catalog, boolean withPooling ) throws IOException, ExecutionException {
    return forConnection(
        replacePooling(
            replaceCatalog(
                MondrianProperties.instance().TestConnectString.get(),
                catalogs.get( catalog ) ),
            withPooling ) );
  }

  public static MondrianContext defaultContext() throws IOException, ExecutionException {
    return forConnection( MondrianProperties.instance().TestConnectString.get() );
  }

  public void verify( final MondrianExpectation expectation ) throws Exception {

    if ( expectation.withFreshCache ) {
      // Make sure to clear the schema cache first.
      olapConnection.unwrap( RolapConnection.class )
        .getCacheControl( null )
        .flushSchemaCache();
    }

    final List<String> sqls = new ArrayList<>();
    RolapUtil.ExecuteQueryHook existingHook = RolapUtil.getHook();
    RolapUtil.setHook( sqlCollector( sqls ) );

    final OlapStatement statement = olapConnection.createStatement();
    if ( expectation.isExpectResultSet() ) {
      // some MDX queries (e.g. drillthrough) return ResultSet object
      ResultSet rs = statement.executeQuery( expectation.getQuery() );
      RolapUtil.setHook( existingHook );
      expectation.verify( rs, sqls, olapConnection.unwrap( RolapConnection.class ).getSchema().getDialect() );
    } else {
      final CellSet cellSet;
      if ( expectation.canBeRandomlyCanceled && Math.random() > 0.5 ) {
        // We have to cancel this query.

        // Create an executor.
        final ExecutorService executor = Util.getExecutorService(
            1, 1, 0, "query-background-canceler", new RejectedExecutionHandler() {
              @Override
              public void rejectedExecution( Runnable r, ThreadPoolExecutor executor ) {
                throw new RuntimeException( "TCK programming error" );
              }
            } );

        // Place the query on the executor thread.
        executor.submit( new Callable<CellSet>() {
          @Override
          public CellSet call() throws Exception {
            return statement.executeOlapQuery( expectation.getQuery() );
          }
        } );

        // Wait a bit.
        Thread.sleep( 1000 );

        // Now cancel the query.
        try {
          statement.cancel();
          statement.close();
        } catch ( Throwable t ) {
          t.printStackTrace();
        }

        cellSet = null;
      } else {
        // No random cancel. Just execute right on this thread.
        cellSet = statement.executeOlapQuery( expectation.getQuery() );
      }

      RolapUtil.setHook( existingHook );

      if ( cellSet != null ) {
        expectation.verify(
            cellSet,
            sqls,
            olapConnection.unwrap( RolapConnection.class ).getSchema().getDialect() );
      }
    }
  }

  private RolapUtil.ExecuteQueryHook sqlCollector( final List<String> sqls ) {
    return new RolapUtil.ExecuteQueryHook() {
      @Override
      public void onExecuteQuery( String sql ) {
        sqls.add( sql );
      }
    };
  }

  private static String replaceCatalog( final String connectString, final Path catalogFile ) {
    return connectString.replaceFirst( "Catalog=[^;]+;", "Catalog=" + catalogFile.toString()
        .replaceAll( "\\\\", "/" ) + ";" );
  }

  private static String replacePooling( final String connectString, final boolean withPooling ) {
    if ( connectString.contains( "PoolNeeded" ) ) {
      return connectString.replaceFirst(
        "PoolNeeded=[^;]+;",
        "PoolNeeded=" + String.valueOf( withPooling ) + ";" );
    } else {
      return connectString.concat( ";PoolNeeded=" + String.valueOf( withPooling ) );
    }
  }

  public static int getMondrianComplianceLevel() {
    return Integer.valueOf(
      testProperties.getProperty( "mondrian.compliance.level" ) );
  }
}
