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
