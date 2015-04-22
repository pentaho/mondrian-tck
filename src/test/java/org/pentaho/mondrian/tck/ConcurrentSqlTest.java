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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

public class ConcurrentSqlTest extends TestBase {

  @Test
  public void testConcurrentSqlSimulatedPool() throws Exception {
    final AtomicReference<Throwable> error =
        new AtomicReference<Throwable>();
    final List<Statement> stmts = Collections
        .synchronizedList( new CopyOnWriteArrayList<Statement>() );

    final ExecutorService es = Executors
      .newCachedThreadPool( new ThreadFactory() {
        public Thread newThread( Runnable r ) {
          final Thread t = Executors.defaultThreadFactory().newThread( r );
          t.setDaemon( true );
          return t;
        }
      } );

    /*
     * We will use a single connection to provide statements. This simulates a
     * connection pool where the same connection can be re-used, as long as we
     * cancel the statements before creating a new statement.
     */
    final Connection conn = SqlContext.defaultContext().connection;

    Runnable r = new Runnable() {
      public void run() {
        try {
          final Statement stmt = conn.createStatement();
          stmts.add( stmt );

          stmt.execute( "select sum(sales_fact_1997.unit_sales) as m0 from sales_fact_1997 sales_fact_1997" );

          final ResultSet resultSet = stmt.getResultSet();
          for ( int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++ ) {
            resultSet.getMetaData().getColumnName( i );
          }

          while ( resultSet.next() ) {
            for ( int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++ ) {
              resultSet.getString( i );
            }
          }

          stmt.close();

        } catch ( Throwable t ) {
          error.set( t );
          t.printStackTrace();
        }
      };

    };

    for ( int i = 0; i < 10; i++ ) {
      es.submit( r );
      Thread.sleep( 500 );
      try {
        stmts.get( i ).cancel();
      } catch (Throwable t) {
        // no op
      }
      Thread.sleep( 100 );
    }

    // Check failure.
    try {
      Assert.assertNull(
        "Driver is not thread safe. Exceptions were encountered.",
        error.get() );
    } finally {
      es.shutdown();
      es.awaitTermination( 30, TimeUnit.SECONDS );
    }
  }
}
