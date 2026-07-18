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
