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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;

public class ConcurrentSqlTest extends TestBase {

  private final SqlExpectation[] queries =
    new SqlExpectation[] {
      SqlExpectation.newBuilder()
        .query( "select unit_sales sum_sales from sales_fact_1997" )
        .columns( "sum_sales" )
        .rows( "2" )
        .partial()
        .cancelTimeout( 1000 )
        .build(),
      SqlExpectation.newBuilder()
        .query( "select unit_sales min_sales from sales_fact_1997" )
        .columns( "min_sales" )
        .rows( "2" )
        .partial()
        .cancelTimeout( 1000 )
        .build(),
      SqlExpectation.newBuilder()
        .query( "select unit_sales max_sales from sales_fact_1997" )
        .columns( "max_sales" )
        .rows( "2" )
        .partial()
        .cancelTimeout( 1000 )
        .build(),
    };

  /*
   * This test is disabled for now. It isn't deterministic and needs more work.
   */
  // @Test( timeout = 120000 )
  public void testConcurrentSqlNoPool() throws Exception {
    runTest( queries, 5, 50, false );
  }

  /*
   * This test is disabled for now. It isn't deterministic and needs more work.
   */
  // @Test( timeout = 120000 )
  public void testConcurrentSqlWithPool() throws Exception {
    runTest( queries, 5, 50, true );
  }

  void runTest(
    final SqlExpectation[] tests,
    int nbThreads,
    int nbRuns,
    final boolean useDBCP ) throws Exception {

    final Random random = new Random();
    final List<Future<Throwable>> futures = new ArrayList<Future<Throwable>>();

    // Create an executor
    final ExecutorService executor =
      Executors.newFixedThreadPool(
        nbThreads,
        new ThreadFactory() {
          private final AtomicInteger counter = new AtomicInteger( 0 );
          public Thread newThread( Runnable r ) {
            final Thread t =
                Executors.defaultThreadFactory().newThread( r );
            t.setDaemon( true );
            t.setName( "ConcurrentSqlTest" + '_' + counter.incrementAndGet() );
            return t;
          }
        } );

    // Send a buncha tasks to be performed.
    for ( int i = 0; i < nbRuns; i++ ) {
      futures.add(
        executor.submit(
          new Callable<Throwable>() {
            public Throwable call() throws Exception {
              try {
                // Create a context with DBCP
                SqlContext sqlContext =
                  useDBCP
                    ? SqlContext.dbcpContext()
                    : SqlContext.defaultContext();

                // Run and validate.
                sqlContext.verify( tests[ random.nextInt( tests.length ) ] );

              } catch ( Throwable t ) {
                return t;
              }
              return null;
            }
          } ) );
    }

    // Iterate over the jobs, but cancel half of them, while for the other
    // half to complete. This will allow to follow where the pool is at and
    // cancel active ones
    Throwable error = null;
    for ( Future<Throwable> future : futures ) {
      final Throwable t = future.get();
      if ( error == null && t != null ) {
        t.printStackTrace();
        error = t;
      }
    }

    // Check the results.
    Assert.assertTrue(
      "Errors detected while running ConcurrentSqlTest:"
        + ( error == null
          ? null
          : error.getMessage() ),
      error == null );
  }
}
