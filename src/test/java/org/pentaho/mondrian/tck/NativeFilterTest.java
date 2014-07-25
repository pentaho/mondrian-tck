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

import static org.pentaho.mondrian.tck.FoodMartCatalogs.*;

public class NativeFilterTest {

  @Test
  public void testHavingClause() throws Exception {
    SqlExpectation expectation = SqlExpectation.newBuilder()
      .query(
        "select customer_id from sales_fact_1997 group by customer_id having sum(sales_fact_1997.unit_sales) > 500" )
      .rows( "4021" )
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
          + "    CASE WHEN sales_fact_1997.customer_id IS NULL THEN 1 ELSE 0 END, sales_fact_1997.customer_id ASC" )
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
          + "order by\n"
          + "    CASE WHEN store.store_country IS NULL THEN 1 ELSE 0 END, store.store_country ASC,\n"
          + "    CASE WHEN store.store_state IS NULL THEN 1 ELSE 0 END, store.store_state ASC,\n"
          + "    CASE WHEN store.store_city IS NULL THEN 1 ELSE 0 END, store.store_city ASC" )
      .build();
    MondrianContext.forCatalog( STAR_WITH_STORE ).verify( expectation );
  }

  @Test
  public void testFilterFunctionSnowflake() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
      .query(
        "select Filter([Product].[Product].[Product Department].members, [Measures].[Unit Sales] > 20000) on 0 from sales" )
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
          + "order by\n"
          + "    CASE WHEN product_class.product_family IS NULL THEN 1 ELSE 0 END, product_class.product_family ASC,\n"
          + "    CASE WHEN product_class.product_department IS NULL THEN 1 ELSE 0 END, "
          + "product_class.product_department ASC" )
      .build();
    MondrianContext.forCatalog( SNOWFLAKE_WITH_PRODUCT ).verify( expectation );
  }

  @Test
  public void testCompoundSlicerAtDifferentLevels() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
      .query(
        "select [Measures].[Customer Count] on 0 from sales " +
          "where ({[Store].[USA].[WA],[Store].[USA].[CA].[San Diego],[Store].[Mexico]})" )
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
        "select sum(sales_fact_1997.unit_sales) as m0\n" +
          "  from store store\n" +
          "     , product product\n" +
          "     , sales_fact_1997 sales_fact_1997\n" +
          " where sales_fact_1997.store_id  = store.store_id\n" +
          "   and sales_fact_1997.product_id = product.product_id\n" +
          "   and ((\n" +
          "           store.store_country         = 'USA'\n" +
          "       and store.first_opened_date = '1979-04-13'\n" +
          "       and store.last_remodel_date = '1982-01-30 00:00:00'\n" +
          "       )\n" +
          "    or (\n" +
          "         store.store_city          = 'San Diego'\n" +
          "     and store.store_state         = 'CA'\n" +
          "       )\n" +
          "    or (\n" +
          "         store.store_state         = 'WA'\n" +
          "     and store.store_sqft          > 50000\n" +
          "     and product.gross_weight      = 17.1\n" +
          "       )\n" +
          "    or (\n" +
          "         store.store_sqft is null\n" +
          "       )\n" +
          "    )\n" )
      .rows( "39329.0" )
      .build();
    sqlContext.verify( expectation );

  }
}
