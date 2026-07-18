/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2013 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/

package org.pentaho.mondrian.tck;

import java.util.List;

public class ConcurrentMdxTest extends TestBase {

  static final QueryAndResult[] mdxQueries = new QueryAndResult[]{
      new QueryAndResult(
        "select {[Measures].[Unit Sales]} on 0 from [Sales] ",
        "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Measures].[Unit Sales]}\n"
        + "Row #0: 266,773\n" ),
      new QueryAndResult(
        "select {[Measures].[Unit Sales]} on 0 from [Sales] where ([customer].[customer id].[500])",
        "Axis #0:\n"
        + "{[customer].[500]}\n"
        + "Axis #1:\n"
        + "{[Measures].[Unit Sales]}\n"
        + "Row #0: 10\n" ),
      new QueryAndResult(
          "select {[Measures].[Unit Sales]} on 0 from [Sales] where ([customer].[customer id].[501])",
          "Axis #0:\n"
          + "{[customer].[501]}\n"
          + "Axis #1:\n"
          + "{[Measures].[Unit Sales]}\n"
          + "Row #0: 32\n" ),
      new QueryAndResult(
          "select {[Measures].[Unit Sales]} on 0 from [Sales] where ([customer].[customer id].[502])",
          "Axis #0:\n"
          + "{[customer].[502]}\n"
          + "Axis #1:\n"
          + "{[Measures].[Unit Sales]}\n"
          + "Row #0: 49\n" ),
      new QueryAndResult(
          "select {[Measures].[Unit Sales]} on 0 from [Sales] where ([customer].[customer id].[504])",
          "Axis #0:\n"
          + "{[customer].[504]}\n"
          + "Axis #1:\n"
          + "{[Measures].[Unit Sales]}\n"
          + "Row #0: 96\n" ),
      new QueryAndResult(
          "select {[Measures].[Unit Sales]} on 0 from [Sales] where ([customer].[customer id].[505])",
          "Axis #0:\n"
          + "{[customer].[505]}\n"
          + "Axis #1:\n"
          + "{[Measures].[Unit Sales]}\n"
          + "Row #0: 47\n" ),
      new QueryAndResult(
          "select {[Measures].[Unit Sales]} on 0 from [Sales] where ([customer].[customer id].[508])",
          "Axis #0:\n"
          + "{[customer].[508]}\n"
          + "Axis #1:\n"
          + "{[Measures].[Unit Sales]}\n"
          + "Row #0: 2\n" ),
      new QueryAndResult(
          "select {[Measures].[Unit Sales]} on 0,\n"
          + "{[store].[store id].members} on 1 from [Sales] ",
          "Axis #0:\n"
          + "{}\n"
          + "Axis #1:\n"
          + "{[Measures].[Unit Sales]}\n"
          + "Axis #2:\n"
          + "{[store].[2]}\n"
          + "{[store].[3]}\n"
          + "{[store].[6]}\n"
          + "{[store].[7]}\n"
          + "{[store].[11]}\n"
          + "{[store].[13]}\n"
          + "{[store].[14]}\n"
          + "{[store].[15]}\n"
          + "{[store].[16]}\n"
          + "{[store].[17]}\n"
          + "{[store].[22]}\n"
          + "{[store].[23]}\n"
          + "{[store].[24]}\n"
          + "Row #0: 2,237\n"
          + "Row #1: 24,576\n"
          + "Row #2: 21,333\n"
          + "Row #3: 25,663\n"
          + "Row #4: 26,079\n"
          + "Row #5: 41,580\n"
          + "Row #6: 2,117\n"
          + "Row #7: 25,011\n"
          + "Row #8: 23,591\n"
          + "Row #9: 35,257\n"
          + "Row #10: 2,203\n"
          + "Row #11: 11,491\n"
          + "Row #12: 25,635\n" )
  };

  /*
   * This test is disabled for now. It isn't deterministic and needs more work.
   */
  // @Test
  public void testConcurrentValidatingQueriesInRandomOrderAndPool() throws Exception {
    final List<Throwable> failures =
        ConcurrentValidatingMdxQueryRunner.runTest( 5, 60, true, true, true, mdxQueries );
    if ( failures.size() > 0 ) {
      // Just throw the first one.
      throw new Exception( failures.get( 0 ) );
    }
  }

  /*
   * This test is disabled for now. It isn't deterministic and needs more work.
   */
  // @Test
  public void testConcurrentValidatingQueriesInRandomOrderNoPool() throws Exception {
    final List<Throwable> failures =
        ConcurrentValidatingMdxQueryRunner.runTest( 5, 60, true, true, false, mdxQueries );
    if ( failures.size() > 0 ) {
      // Just throw the first one.
      throw new Exception( failures.get( 0 ) );
    }
  }
}
