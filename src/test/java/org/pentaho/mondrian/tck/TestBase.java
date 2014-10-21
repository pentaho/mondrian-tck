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
