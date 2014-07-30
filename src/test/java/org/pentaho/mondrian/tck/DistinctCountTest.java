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
      .rows( "5581" )
      .build();
    sqlContext.verify( sqlExpectation );
  }

  @Test
  public void testMultipleColumnSQL() throws Exception {
    SqlContext sqlContext = SqlContext.defaultContext();
    SqlExpectation sqlExpectation = SqlExpectation.newBuilder()
      .query( "select count(distinct(customer_id)), count(distinct(product_id))  from sales_fact_1997" )
      .rows( "5581", "1559" )
      .build();
    sqlContext.verify( sqlExpectation );
  }

  @Test
  public void testCompoundColumnSQL() throws Exception {
    SqlContext sqlContext = SqlContext.defaultContext();
    SqlExpectation sqlExpectation = SqlExpectation.newBuilder()
      .query( "select count( distinct customer_id, product_id) from sales_fact_1997" )
      .rows( "85452" )
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
          public ResultSet getData( Statement statement ) throws Exception {
            return statement.getConnection().getMetaData().getIndexInfo( statement.getConnection().getCatalog(), null, "sales_fact_1997", false, false );
          }
        } )

        .columns( "table_cat", "table_schem", "table_name", "non_unique", "index_qualifier", "index_name", "type",
            "ordinal_position", "column_name", "asc_or_desc", "cardinality", "pages", "filter_condition" )

        .types( Types.CHAR, Types.CHAR, Types.CHAR, Types.BOOLEAN, Types.CHAR, Types.CHAR, Types.SMALLINT,
            Types.SMALLINT, Types.CHAR, Types.CHAR, Types.INTEGER, Types.INTEGER, Types.CHAR )

        .rows( "foodmart|null|sales_fact_1997|true||i_sls_97_cust_id|3|1|customer_id|A|9652|0|null" )

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
        .build();
    MondrianContext.forCatalog( FoodMartCatalogs.FLAT_WITH_CUSTOMER.replace( "aggregator=\"sum\""
        , "aggregator=\"distinct-count\"" ) ).verify( expectation );
  }
}
