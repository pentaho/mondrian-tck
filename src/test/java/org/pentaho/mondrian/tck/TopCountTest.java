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

import com.google.common.base.Function;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

public class TopCountTest {
  @Test
  public void testSetMaxRows() throws Exception {
    SqlContext sqlContext = SqlContext.defaultContext();
    SqlExpectation sqlExpectation = SqlExpectation.newBuilder()
      .query( "select unit_sales from sales_fact_1997 order by unit_sales desc" )
      .modifyStatement( new Function<Statement, Void>() {
        @Override
        public Void apply( final Statement statement ) {
          try {
            statement.setMaxRows( 2 );
          } catch ( SQLException e ) {
            fail( "Should have been able to setMaxRows" );
          }
          return null;
        }
      } )
      .rows( "6.0", "6.0" )
      .build();
    sqlContext.verify( sqlExpectation );
  }

  @Test
  public void testTopCount() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
      .query(
        "select "
          + "  non empty TopCount([customer].[customer].[customer id].members,5,[Measures].[Unit Sales]) on 0,"
          + "  [Measures].[Unit Sales] on 1 "
          + "  from Sales" )
      .result(
        "Axis #0:\n"
          + "{}\n"
          + "Axis #1:\n"
          + "{[customer].[4021]}\n"
          + "{[customer].[8452]}\n"
          + "{[customer].[5295]}\n"
          + "{[customer].[4727]}\n"
          + "{[customer].[1297]}\n"
          + "Axis #2:\n"
          + "{[Measures].[Unit Sales]}\n"
          + "Row #0: 518\n"
          + "Row #0: 447\n"
          + "Row #0: 441\n"
          + "Row #0: 439\n"
          + "Row #0: 392\n" )
      .sql(
        "select\n"
          + "    sales_fact_1997.customer_id as c0,\n"
          + "    sum(sales_fact_1997.unit_sales) as c1\n"
          + "from\n"
          + "    sales_fact_1997 sales_fact_1997\n"
          + "group by\n"
          + "    sales_fact_1997.customer_id\n"
          + "order by\n"
          + "    CASE WHEN sum(sales_fact_1997.unit_sales) IS NULL THEN 1 ELSE 0 END, sum(sales_fact_1997.unit_sales) DESC,\n"
          + "    CASE WHEN sales_fact_1997.customer_id IS NULL THEN 1 ELSE 0 END, sales_fact_1997.customer_id ASC" )
      .build();
    MondrianContext.defaultContext().withCatalog(
      "<Schema name=\"FoodMart\">"
        + "  <Cube name=\"Sales\" defaultMeasure=\"Unit Sales\">"
        + "    <Table name=\"sales_fact_1997\"/>"
        + "  <Dimension name=\"customer\">\n"
        + "    <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
        + "      <Table name=\"sales_fact_1997\"/>\n"
        + "      <Level name=\"customer id\" type=\"Integer\" column=\"customer_id\" uniqueMembers=\"true\"/>\n"
        + "    </Hierarchy>\n"
        + "  </Dimension>"
        + "    <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>"
        + "  </Cube>"
        + "</Schema>" ).verify( expectation );
  }
}
