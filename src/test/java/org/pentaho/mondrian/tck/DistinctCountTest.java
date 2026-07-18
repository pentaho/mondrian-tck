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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;

import org.junit.Test;

public class DistinctCountTest {

  @Test
  public void testSingleColumnSQL() throws Exception {
    SqlContext sqlContext = SqlContext.defaultContext();
    SqlExpectation sqlExpectation = SqlExpectation.newBuilder()
        .query( "select count(distinct(customer_id)) from sales_fact_1997" )
        .rows( "5,581" )
        .build();
    sqlContext.verify( sqlExpectation );
  }

  @Test
  public void testMultipleColumnSQL() throws Exception {
    SqlContext sqlContext = SqlContext.defaultContext();
    SqlExpectation sqlExpectation = SqlExpectation.newBuilder()
        .query( "select count(distinct(customer_id)), count(distinct(product_id))  from sales_fact_1997" )
        .rows( "5,581|1,559" )
        .build();
    sqlContext.verify( sqlExpectation );
  }

  @Test
  public void testCompoundColumnSQL() throws Exception {
    SqlContext sqlContext = SqlContext.defaultContext();
    SqlExpectation sqlExpectation = SqlExpectation.newBuilder()
        .query( "select count( distinct customer_id, product_id) from sales_fact_1997" )
        .rows( "85,452" )
        .build();
    sqlContext.verify( sqlExpectation );
  }

  /**
   * Test support of JDBC driver getIndexInfo.
   * Driver supports this function if no exception was thrown.
   *
   * tested with MySQL
   *
   * @throws Exception
   */
  @Test
  public void testJDBCIndexes() throws Exception {

    SqlExpectation expectation = SqlExpectation.newBuilder()
        .query( new SqlExpectation.ResultSetProvider() {
          @Override
          public ResultSet getData( Connection conn, Statement statement ) throws Exception {
            return conn.getMetaData().getIndexInfo( conn.getCatalog(), null, "product", false, false );
          }
        } )

        .columns( "table_cat", "table_schem", "table_name", "non_unique", "index_qualifier", "index_name", "type",
            "ordinal_position", "column_name", "asc_or_desc", "cardinality", "pages", "filter_condition" )

        .types( Types.CHAR, Types.CHAR, Types.CHAR, Types.BOOLEAN, Types.CHAR, Types.CHAR, Types.SMALLINT,
            Types.SMALLINT, Types.CHAR, Types.CHAR, Types.INTEGER, Types.INTEGER, Types.CHAR )

        .rows( "foodmart|null|product|false||i_product_id|3|1|product_id|A|1,560|0|null" )

        .partial()
        .build();

    SqlContext.defaultContext().verify( expectation );
  }

  @Test
  public void testSingleColumnMondrian() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
        .query( "select [Measures].[Unit Sales] as unit_sales on 0 from Sales" )
        .result( "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 6\n" )
        .sql( "select\n"
            + "    count(distinct sales_fact_1997.unit_sales) as m0\n"
            + "from\n"
            + "    sales_fact_1997 sales_fact_1997" )
        // This test needs a fresh cache, or it pulls the column metadata
        // from other tests.
        .withFreshCache()
        .build();
    MondrianContext.forCatalog( FoodMartCatalogs.FLAT_WITH_CUSTOMER.replace( "aggregator=\"sum\""
        , "aggregator=\"distinct-count\"" ) ).verify( expectation );
  }
}
