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

/**
 * Test "GROUPING SETS" clause. Fails at Impala as it doesn't support this syntax.
 */
public class GroupingSetTest {

  public static final String MDX = "with member [Gender].[agg] as ' "
    + "  Aggregate({[Gender].DefaultMember}, [Measures].[Store Cost])' "
    + "select "
    + "  {[Measures].[Store Cost]} ON COLUMNS, "
    + "  {[Gender].[Gender].Members, [Gender].[agg]} ON ROWS "
    + "from [Sales]";

  /**
   * Test "grouping sets ((col1))"
   */
  @Test
  public void testPlainEntry() throws Exception {
    String query = getGSetSQLQuery(
      "customer.gender as gender, sum(sales_fact_1997.store_cost) as sum_cost",
      "(customer.gender)" );
    SqlExpectation expectation = SqlExpectation.newBuilder()
      .query( query )
      .columns( "gender", "sum_cost" )
      .rows( "M|113849.7546000008", "F|111777.47900000079" )
      .build();
    SqlContext.defaultContext().verify( expectation );
  }

  /**
   * Test "grouping sets ((col1,col2))"
   */
  @Test
  public void testComplexEntry() throws Exception {
    String query = getGSetSQLQuery(
      "time_by_day.the_year as the_year, customer.gender as gender, sum(sales_fact_1997.store_cost) as sum_cost",
      "(time_by_day.the_year, customer.gender)" );
    SqlExpectation expectation = SqlExpectation.newBuilder()
      .query( query )
      .columns( "the_year", "gender", "sum_cost" )
      .rows( "1997|M|113849.7546000008", "1997|F|111777.47900000079" )
      .build();
    SqlContext.defaultContext().verify( expectation );
  }

  /**
   * Test "grouping sets ((col1), ())"
   */
  @Test
  public void testEmptyEntry() throws Exception {
    String query = getGSetSQLQuery(
      "customer.gender as gender, sum(sales_fact_1997.store_cost) as sum_cost",
      "(customer.gender),()" );
    SqlExpectation expectation =
      SqlExpectation.newBuilder()
        .query( query )
        .columns( "gender", "sum_cost" )
        .rows( "M|113849.7546000008", "F|111777.47900000079", "null|225627.2336000015" )
        .build();
    SqlContext.defaultContext().verify( expectation );
  }

  /**
   * Test "grouping sets ((col1,col2),(col3))"
   */
  @Test
  public void testMultipleEntries() throws Exception {
    String query = getGSetSQLQuery(
      "time_by_day.the_year as the_year, customer.gender as gender, sum(sales_fact_1997.store_cost) as sum_cost",
      "(time_by_day.the_year, customer.gender), (time_by_day.the_year),()" );
    SqlExpectation expectation = SqlExpectation.newBuilder()
      .query( query )
      .columns( "the_year", "gender", "sum_cost" )
      .rows( "1997|M|113849.7546000008", "1997|F|111777.47900000079", "null|225627.2336000015", "null|225627.2336000015" )
      .build();
    SqlContext.defaultContext().verify( expectation );
  }

  private String getGSetSQLQuery( String columns, String conditions ) {
    return "select " + columns + "\n"
      + "from time_by_day, sales_fact_1997, customer \n"
      + "where (sales_fact_1997.time_id = time_by_day.time_id and time_by_day.the_year = 1997\n"
      + "and sales_fact_1997.customer_id = customer.customer_id)\n"
      + "group by grouping sets (" + conditions + ")";
  }

  @Test
  public void testGSetMondrian() throws Exception {
    MondrianExpectation expectation =
      MondrianExpectation.newBuilder()
        .query( MDX )
        .result(
          "Axis #0:\n"
          + "{}\n"
          + "Axis #1:\n"
          + "{[Measures].[Store Cost]}\n"
          + "Axis #2:\n"
          + "{[Gender].[F]}\n"
          + "{[Gender].[M]}\n"
          + "{[Gender].[agg]}\n"
          + "Row #0: 111,777.48\n"
          + "Row #1: 113,849.75\n"
          + "Row #2: 225,627.23\n" )
        .build();
    MondrianContext.defaultContext().verify( expectation );
  }

}
