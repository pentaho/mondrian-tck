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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import mondrian.olap.MondrianProperties;
import mondrian.rolap.RolapConnection;
import mondrian.rolap.RolapUtil;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapStatement;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

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

  private OlapConnection olapConnection;

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

  public static MondrianContext defaultContext() throws IOException, ExecutionException {
    return forConnection( MondrianProperties.instance().TestConnectString.get() );
  }

  public void verify( MondrianExpectation expectation ) throws Exception {
    final List<String> sqls = new ArrayList<>();
    RolapUtil.ExecuteQueryHook existingHook = RolapUtil.getHook();
    RolapUtil.setHook( sqlCollector( sqls ) );

    OlapStatement statement = olapConnection.createStatement();
    CellSet cellSet = statement.executeOlapQuery( expectation.getQuery() );
    RolapUtil.setHook( existingHook );

    expectation.verify(
      cellSet,
      sqls,
      olapConnection.unwrap( RolapConnection.class ).getSchema().getDialect() );
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
    return connectString.replaceFirst( "Catalog=[^;]+;", "Catalog=" + catalogFile.toString() + ";" );
  }
}
