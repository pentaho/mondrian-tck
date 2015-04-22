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

import static org.pentaho.mondrian.tck.FoodMartCatalogs.*;

public class NativeFilterTest extends TestBase {

  @Test
  public void testHavingClause() throws Exception {
    SqlExpectation expectation = SqlExpectation.newBuilder()
      .query(
        "select customer_id from sales_fact_1997 group by customer_id having sum(sales_fact_1997.unit_sales) > 500" )
      .rows( "4,021" )
      .build();
    SqlContext.defaultContext().verify( expectation );
  }

  @Test
  public void testFilterFunctionSingleFact() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
      .query(
        "select Filter([customer].[customer].[customer id].members, [Measures].[Unit Sales] > 500) on 0 from sales" )
      .result(
        "Axis #0:\n"
          + "{}\n"
          + "Axis #1:\n"
          + "{[customer].[4021]}\n"
          + "Row #0: 518\n" )
      .sql(
        "select\n"
          + "    sales_fact_1997.customer_id as c0\n"
          + "from\n"
          + "    sales_fact_1997 sales_fact_1997\n"
          + "group by\n"
          + "    sales_fact_1997.customer_id\n"
          + "having\n"
          + "    (sum(sales_fact_1997.unit_sales) > 500)\n"
          + "order by\n"
          + "    " + getOrderExpression( "c0", "sales_fact_1997.customer_id", true, true, true ) )
      .build();
    MondrianContext.forCatalog( FLAT_WITH_CUSTOMER ).verify( expectation );
  }

  @Test
  public void testFilterFunctionStar() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
      .query(
        "select Filter([Store].[Store].[Store City].members, [Measures].[Unit Sales] > 20000) on 0 from sales" )
      .result(
        "Axis #0:\n"
          + "{}\n"
          + "Axis #1:\n"
          + "{[Store].[USA].[CA].[Beverly Hills]}\n"
          + "{[Store].[USA].[CA].[Los Angeles]}\n"
          + "{[Store].[USA].[CA].[San Diego]}\n"
          + "{[Store].[USA].[OR].[Portland]}\n"
          + "{[Store].[USA].[OR].[Salem]}\n"
          + "{[Store].[USA].[WA].[Bremerton]}\n"
          + "{[Store].[USA].[WA].[Seattle]}\n"
          + "{[Store].[USA].[WA].[Spokane]}\n"
          + "{[Store].[USA].[WA].[Tacoma]}\n"
          + "Row #0: 21,333\n"
          + "Row #0: 25,663\n"
          + "Row #0: 25,635\n"
          + "Row #0: 26,079\n"
          + "Row #0: 41,580\n"
          + "Row #0: 24,576\n"
          + "Row #0: 25,011\n"
          + "Row #0: 23,591\n"
          + "Row #0: 35,257\n" )
      .sql(
        "select\n"
          + "    store.store_country as c0,\n"
          + "    store.store_state as c1,\n"
          + "    store.store_city as c2\n"
          + "from\n"
          + "    store store,\n"
          + "    sales_fact_1997 sales_fact_1997\n"
          + "where\n"
          + "    sales_fact_1997.store_id = store.store_id\n"
          + "group by\n"
          + "    store.store_country,\n"
          + "    store.store_state,\n"
          + "    store.store_city\n"
          + "having\n"
          + "    (sum(sales_fact_1997.unit_sales) > 20000)\n"
          + "order by"
          + "\n    " + getOrderExpression( "c0", "store.store_country", true, true, true )
          + ",\n    " + getOrderExpression( "c1", "store.store_state", true, true, true )
          + ",\n    " + getOrderExpression( "c2", "store.store_city", true, true, true ) )
      .build();
    MondrianContext.forCatalog( STAR_WITH_STORE ).verify( expectation );
  }

  @Test
  public void testFilterFunctionSnowflake() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
      .query(
        "select Filter([Product].[Product].[Product Department].members, [Measures].[Unit Sales] > 20000) on 0 "
          + "from sales" )
      .result(
        "Axis #0:\n"
          + "{}\n"
          + "Axis #1:\n"
          + "{[Product].[Food].[Baking Goods]}\n"
          + "{[Product].[Food].[Frozen Foods]}\n"
          + "{[Product].[Food].[Produce]}\n"
          + "{[Product].[Food].[Snack Foods]}\n"
          + "{[Product].[Non-Consumable].[Household]}\n"
          + "Row #0: 20,245\n"
          + "Row #0: 26,655\n"
          + "Row #0: 37,792\n"
          + "Row #0: 30,545\n"
          + "Row #0: 27,038\n" )
      .sql(
        "select\n"
          + "    product_class.product_family as c0,\n"
          + "    product_class.product_department as c1\n"
          + "from\n"
          + "    product product,\n"
          + "    product_class product_class,\n"
          + "    sales_fact_1997 sales_fact_1997\n"
          + "where\n"
          + "    product.product_class_id = product_class.product_class_id\n"
          + "and\n"
          + "    sales_fact_1997.product_id = product.product_id\n"
          + "group by\n"
          + "    product_class.product_family,\n"
          + "    product_class.product_department\n"
          + "having\n"
          + "    (sum(sales_fact_1997.unit_sales) > 20000)\n"
          + "order by"
          + "\n    " + getOrderExpression( "c0", "product_class.product_family", true, true, true )
          + ",\n    " + getOrderExpression( "c1", "product_class.product_department", true, true, true ) )
      .build();
    MondrianContext.forCatalog( SNOWFLAKE_WITH_PRODUCT ).verify( expectation );
  }

  @Test
  public void testCompoundSlicerAtDifferentLevels() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
      .query(
        "select [Measures].[Customer Count] on 0 from sales "
        + "where ({[Store].[USA].[WA],[Store].[USA].[CA].[San Diego],[Store].[Mexico]})" )
      .result(
        "Axis #0:\n"
          + "{[Store].[USA].[WA]}\n"
          + "{[Store].[USA].[CA].[San Diego]}\n"
          + "{[Store].[Mexico]}\n"
          + "Axis #1:\n"
          + "{[Measures].[Customer Count]}\n"
          + "Row #0: 2,790\n" )
      .sql(
        "select\n"
          + "    count(distinct sales_fact_1997.customer_id) as m0\n"
          + "from\n"
          + "    store store,\n"
          + "    sales_fact_1997 sales_fact_1997\n"
          + "where\n"
          + "    sales_fact_1997.store_id = store.store_id\n"
          + "and\n"
          + "    (store.store_state = 'WA' "
          + "or (store.store_city = 'San Diego' and store.store_state = 'CA') "
          + "or store.store_country = 'Mexico')" )
      .build();
    MondrianContext.forCatalog( STAR_WITH_STORE ).verify( expectation );

  }

  @Test
  public void testCompoundPredicate() throws Exception {
    SqlContext sqlContext = SqlContext.defaultContext();
    SqlExpectation expectation = SqlExpectation.newBuilder()
      .query(
        "select sum(sales_fact_1997.unit_sales) as m0\n"
          + "  from store store\n"
          + "     , product product\n"
          + "     , sales_fact_1997 sales_fact_1997\n"
          + " where sales_fact_1997.store_id  = store.store_id\n"
          + "   and sales_fact_1997.product_id = product.product_id\n"
          + "   and ((\n"
          + "           store.store_country     = " + quoteString( "USA" ) + "\n"
          + "       and store.first_opened_date = " + quoteDate( "1981-01-03" ) + "\n"
          + "       and store.last_remodel_date = " + quoteTimestamp( "1991-03-13 00:00:00" ) + "\n"
          + "       )\n"
          + "    or (\n"
          + "         store.store_city          = " + quoteString( "San Diego" ) + "\n"
          + "     and store.store_state         = " + quoteString( "CA" ) + "\n"
          + "       )\n"
          + "    or (\n"
          + "         store.store_state         = " + quoteString( "WA" ) + "\n"
          + "     and store.store_sqft          > 50000\n"
          + "     and product.gross_weight      = 17.1\n"
          + "       )\n"
          + "    or (\n"
          + "         store.store_sqft is null\n"
          + "       )\n"
          + "    )\n" )
      .rows( "60,662" )
      .build();
    sqlContext.verify( expectation );

  }

  @Test
  public void testCompoundPredicateNoJoinsDateLiteralSyntax() throws Exception {
    SqlContext.defaultContext().verify( SqlExpectation.newBuilder()
      .query(
        "select sum(store.store_sqft) as m0\n"
          + "  from store store\n"
          + " where (\n"
          + "           store.store_country     = " + quoteString( "USA" ) + "\n"
          + "       and store.first_opened_date = " + quoteDate( "1981-01-03" ) + "\n"
          + "       and store.last_remodel_date = " + quoteTimestamp( "1991-03-13 00:00:00" ) + "\n"
          + "       )\n"
          + "    or (\n"
          + "         store.store_city        = " + quoteString( "San Diego" ) + "\n"
          + "     and store.store_state       = " + quoteString( "CA" ) + "\n"
          + "       )\n"
          + "    or (\n"
          + "         store.store_state       = " + quoteString( "WA" ) + "\n"
          + "     and store.store_sqft        > 30000\n"
          + "       )\n"
          + "    or ( store.store_sqft is null)\n" )
      .rows( "127,510" )
      .build() );
  }
}
