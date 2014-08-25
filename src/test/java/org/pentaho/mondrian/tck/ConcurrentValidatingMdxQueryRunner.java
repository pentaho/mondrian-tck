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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import mondrian.rolap.RolapConnection;

import org.apache.log4j.Logger;
import org.pentaho.mondrian.tck.TestBase.QueryAndResult;

public class ConcurrentValidatingMdxQueryRunner extends Thread {
  private static final Logger LOGGER = Logger.getLogger( ConcurrentValidatingMdxQueryRunner.class );
  private long mRunTime;
  private long mStartTime;
  private long mStopTime;
  private volatile List<Throwable> mExceptions = new ArrayList<Throwable>();
  private String threadName;
  private int mRunCount;
  private int mSuccessCount;
  private boolean mRandomQueries;

  private QueryAndResult[] mdxQueries;
  private boolean usePooling;

  /**
   * Runs concurrent queries without flushing cache. This constructor
   * provides backward compatibilty for usage in {@link ConcurrentMdxTest}.
   *
   * @param numSeconds Running time
   * @param useRandomQuery If set to <code>true</code>, the runner will
   *        pick a random query from the set. If set to <code>false</code>,
   *        the runner will circle through queries sequentially
   * @param queriesAndResults The array of pairs of query and expected result
   */
  public ConcurrentValidatingMdxQueryRunner(
    int numSeconds,
    boolean useRandomQuery,
    boolean usePooling,
    QueryAndResult[] queriesAndResults ) {
    this.mdxQueries = queriesAndResults;
    this.mRunTime = numSeconds * 1000;
    this.mRandomQueries = useRandomQuery;
    this.usePooling = usePooling;
  }

  /**
   * Runs a number of queries until time expires. For each iteration,
   * if cache is to be flushed, do it before running the query.
   */
  public void run() {
    mStartTime = System.currentTimeMillis();
    threadName = Thread.currentThread().getName();
    try {
      int queryIndex = -1;

      while ( System.currentTimeMillis() - mStartTime < mRunTime ) {
        try {
          if ( mRandomQueries ) {
            queryIndex =
              (int) ( Math.random() * mdxQueries.length );
          } else {
            queryIndex = mRunCount % mdxQueries.length;
          }

          mRunCount++;

          MondrianExpectation expectation = MondrianExpectation.newBuilder()
            .query( mdxQueries[queryIndex].query )
            .result( mdxQueries[queryIndex].result )
            .canBeRandomlyCanceled()
            .build();
          MondrianContext
            .forCatalog( FoodMartCatalogs.FLAT_WITH_FEW_DIMS, usePooling )
            .verify( expectation );

          mSuccessCount++;

        } catch ( Throwable e ) {
          mExceptions.add(
            new Exception(
                "Exception occurred in iteration " + mRunCount
                + " of thread " + Thread.currentThread().getName(),
                e ) );
        }
      }
      mStopTime = System.currentTimeMillis();
    } catch ( Throwable e ) {
      mExceptions.add( e );
    }
  }

  /**
   * Prints result of this test run.
   */
  private void report() {
    String message = MessageFormat.format(
      " {0} ran {1} queries, {2} successfully in {3} milliseconds",
      threadName,
      mRunCount,
      mSuccessCount,
      mStopTime - mStartTime );

    LOGGER.info( message );

    for ( Object throwable : mExceptions ) {
      LOGGER.error( throwable );
    }
  }

  /**
   * Creates and runs concurrent threads of tests
   *
   * @param numThreads Number of concurrent threads
   * @param runTimeInSeconds Running Time
   * @param randomQueries Whether to pick queries in random or in sequence
   * @param printReport Whether to print report
   * @param queriesAndResults Array of pairs of query and expected result
   * @return The list of failures
   */
  static List<Throwable> runTest(
      int numThreads,
      int runTimeInSeconds,
      boolean randomQueries,
      boolean printReport,
      boolean usePooling,
      QueryAndResult[] queriesAndResults ) throws Exception {

    // Make sure to clear the schema cache first.
    MondrianContext
      .forCatalog( FoodMartCatalogs.FLAT_WITH_FEW_DIMS, usePooling )
        .olapConnection.unwrap( RolapConnection.class )
          .getCacheControl( null )
            .flushSchemaCache();

    ConcurrentValidatingMdxQueryRunner[] runners =
      new ConcurrentValidatingMdxQueryRunner[numThreads];
    List<Throwable> allExceptions = new ArrayList<Throwable>();

    for ( int idx = 0; idx < runners.length; idx++ ) {
      runners[idx] = new ConcurrentValidatingMdxQueryRunner(
        runTimeInSeconds,
        randomQueries,
        usePooling,
        queriesAndResults );
    }

    for ( int idx = 0; idx < runners.length; idx++ ) {
      runners[idx].start();
    }

    for ( int idx = 0; idx < runners.length; idx++ ) {
      try {
        runners[idx].join();
      } catch ( InterruptedException e ) {
        e.printStackTrace();
      }
    }

    for ( int idx = 0; idx < runners.length; idx++ ) {
      allExceptions.addAll( runners[idx].mExceptions );
      if ( printReport ) {
        runners[idx].report();
      }
    }
    return allExceptions;
  }
}
