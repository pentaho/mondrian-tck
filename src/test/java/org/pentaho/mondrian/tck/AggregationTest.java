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
