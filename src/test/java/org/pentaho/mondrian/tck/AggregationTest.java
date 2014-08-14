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

import org.junit.Test;

import java.sql.Types;

import static org.pentaho.mondrian.tck.SqlExpectation.newBuilder;

public class AggregationTest {

  public static final String QUERY = "select "
      + "  [Measures].[Unit Sales] as unit_sales on 0 "
      + "  from Sales";

  /**
   * This test verifies that we can use SUM aggregator
   */
  @Test
  public void testSum() throws Exception {
    final SqlExpectation expectation = newBuilder()
        .query( "select sum(unit_sales) sum_sales from sales_fact_1997" )
        .columns( "sum_sales" )
        .rows( "266,773" )
        .build();

    // Validate
    SqlContext.defaultContext().verify( expectation );
  }

  @Test
  public void testSumMondrian() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
        .query( QUERY )
        .result( getResult( "266,773" ) )
        .sql( getSql( "sum" ) )
        .build();
    MondrianContext.forCatalog( FoodMartCatalogs.FLAT_WITH_CUSTOMER ).verify( expectation );
  }

  /**
   * This test verifies that we can use MIN aggregator
   */
  @Test
  public void testMin() throws Exception {
    final SqlExpectation expectation = newBuilder()
        .query( "select min(unit_sales) min_sales from sales_fact_1997" )
        .columns( "min_sales" )
        .rows( "1" )
        .build();

    // Validate
    SqlContext.defaultContext().verify( expectation );
  }

  @Test
  public void testMinMondrian() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
        .query( QUERY )
        .result( getResult( "1" ) )
        .sql( getSql( "min" ) )
        .build();
    MondrianContext.forCatalog( FoodMartCatalogs.FLAT_WITH_CUSTOMER.replace( "aggregator=\"sum\""
        , "aggregator=\"min\"" ) ).verify( expectation );
  }

  /**
   * This test verifies that we can use MAX aggregator
   */
  @Test
  public void testMax() throws Exception {
    final SqlExpectation expectation = newBuilder()
        .query( "select max(unit_sales) max_sales from sales_fact_1997" )
        .columns( "max_sales" )
        .rows( "6" )
        .build();

    // Validate
    SqlContext.defaultContext().verify( expectation );
  }

  @Test
  public void testMaxMondrian() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
        .query( QUERY )
        .result( getResult( "6" ) )
        .sql( getSql( "max" ) )
        .build();
    MondrianContext.forCatalog( FoodMartCatalogs.FLAT_WITH_CUSTOMER.replace( "aggregator=\"sum\""
        , "aggregator=\"max\"" ) ).verify( expectation );
  }

  /**
   * This test verifies that we can use COUNT aggregator
   */
  @Test
  public void testCount() throws Exception {
    final SqlExpectation expectation = newBuilder()
        .query( "select count(unit_sales) count_sales from sales_fact_1997" )
        .columns( "count_sales" )
        .types( Types.BIGINT )
        .rows( String.valueOf( 86837 ) )
        .build();

    // Validate
    SqlContext.defaultContext().verify( expectation );
  }

  @Test
  public void testCountMondrian() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
        .query( QUERY )
        .result( getResult( "86,837" ) )
        .sql( getSql( "count" ) )
        .build();
    MondrianContext.forCatalog( FoodMartCatalogs.FLAT_WITH_CUSTOMER.replace( "aggregator=\"sum\""
        , "aggregator=\"count\"" ) ).verify( expectation );
  }

  /**
   * This test verifies that we can use COUNT( DISTINCT col ) aggregator
   */
  @Test
  public void testDistinctCount() throws Exception {
    final SqlExpectation expectation = newBuilder()
        .query( "select count(distinct unit_sales) count_sales from sales_fact_1997" )
        .columns( "count_sales" )
        .types( Types.BIGINT )
        .rows( String.valueOf( 6 ) )
        .build();

    // Validate
    SqlContext.defaultContext().verify( expectation );
  }

  @Test
  public void testCountDistinctMondrian() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
        .query( QUERY )
        .result( getResult( "6" ) )
        .sql( getSql( "count", true ) )
        .build();
    MondrianContext.forCatalog( FoodMartCatalogs.FLAT_WITH_CUSTOMER.replace( "aggregator=\"sum\""
        , "aggregator=\"distinct-count\"" ) ).verify( expectation );
  }

  /**
   * This test verifies that we can use COUNT( DISTINCT col1, col2 ) aggregator
   */
  @Test
  public void testDistinctCountTwoColumns() throws Exception {
    final SqlExpectation expectation = newBuilder()
        .query( "select count(distinct unit_sales, store_id) count_sales from sales_fact_1997" )
        .columns( "count_sales" )
        .types( Types.BIGINT )
        .rows( String.valueOf( 59 ) )
        .build();

    // Validate
    SqlContext.defaultContext().verify( expectation );
  }

  /**
   * <p>This test verifies that we can use COUNT( DISTINCT col1, col2 ) aggregator</p>
   *
   * <p>However this test will fail because of
   * <a href="https://issues.cloudera.org/browse/IMPALA-110">https://issues.cloudera.org/browse/IMPALA-110</a></p>
   */
  @Test
  public void testDistinctTwoCount() throws Exception {
    final SqlExpectation expectation = newBuilder()
        .query( "select count(distinct unit_sales) count_sales, count(distinct store_id) count_store_id "
            + "from sales_fact_1997" )
        .columns( "count_sales", "count_store_id" )
        .types( Types.BIGINT, Types.BIGINT )
        .rows( "6|13" )
        .build();

    // Validate
    SqlContext.defaultContext().verify( expectation );
  }

  /**
   * This test verifies that we can use COUNT( * ) aggregator
   */
  @Test
  public void testCountAll() throws Exception {
    final SqlExpectation expectation = newBuilder()
        .query( "select count(*) count_all from sales_fact_1997" )
        .columns( "count_all" )
        .types( Types.BIGINT )
        .rows( String.valueOf( 86837 ) )
        .build();

    // Validate
    SqlContext.defaultContext().verify( expectation );
  }

  /**
   * This test verifies that we can use AVG aggregator
   */
  @Test
  public void testAvg() throws Exception {
    final SqlExpectation expectation = newBuilder()
        .query( "select avg(unit_sales) avg_sales from sales_fact_1997" )
        .columns( "avg_sales" )
        .rows( String.valueOf( 3.072 ) )
        .build();

    // Validate
    SqlContext.defaultContext().verify( expectation );
  }

  @Test
  public void testAvgMondrian() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
        .query( QUERY )
        .result( getResult( "3.07" ) )
        .sql( getSql( "avg" ) )
        .build();
    MondrianContext.forCatalog( FoodMartCatalogs.FLAT_WITH_CUSTOMER.replace( "aggregator=\"sum\" "
        + "formatString=\"Standard\"", "aggregator=\"avg\" formatString=\"#,###.00\"" ) ).verify( expectation );
  }

  private String getResult( String expectedResult ) {
    return "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Measures].[Unit Sales]}\n"
        + "Row #0: " + expectedResult + "\n";
  }

  private String getSql( String aggregate, boolean distinct ) {
    return "select\n"
        + "    " + aggregate + "(" + ( distinct ? "distinct " : "" ) + "sales_fact_1997.unit_sales) as m0\n"
        + "from\n"
        + "    sales_fact_1997 sales_fact_1997";
  }

  private String getSql( String aggregate ) {
    return getSql( aggregate, false );
  }
}
