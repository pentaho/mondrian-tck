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

import mondrian.spi.Dialect;

import org.junit.BeforeClass;

public abstract class TestBase {

  static Dialect dialect;

  @BeforeClass
  public static void setUp() throws Exception {
    dialect = SqlContext.defaultContext().getDialect();
  }

  static class QueryAndResult {
    final String query;
    final String result;

    QueryAndResult( String query, String result ) {
      this.query = query;
      this.result = result;
    }
  }

  String generateSelectQuery(
      String selectParam,
      String fromParam,
      String orderExpr ) {
    StringBuilder query = new StringBuilder( "select " );
    query.append( dialect.quoteIdentifier( selectParam ) );
    query.append( " from " );
    query.append( dialect.quoteIdentifier( fromParam ) );
    query.append( " order by " );
    query.append( orderExpr );
    return query.toString();
  }

  String getOrderExpression(
      String alias,
      String expr,
      boolean nullable,
      boolean ascending,
      boolean collateNullsLast ) {
    return
      dialect.generateOrderItem(
        dialect.requiresOrderByAlias()
          ? dialect.quoteIdentifier( alias )
          : dialect.quoteIdentifier( expr ),
        nullable,
        ascending,
        collateNullsLast );
  }

  String quoteString( String source ) {
    final StringBuilder sb = new StringBuilder();
    dialect.quoteStringLiteral( sb, source );
    return sb.toString();
  }

  String quoteDate( String source ) {
    final StringBuilder sb = new StringBuilder();
    dialect.quoteDateLiteral( sb, source );
    return sb.toString();
  }

  String quoteTimestamp( String source ) {
    final StringBuilder sb = new StringBuilder();
    dialect.quoteTimestampLiteral( sb, source );
    return sb.toString();
  }
}
