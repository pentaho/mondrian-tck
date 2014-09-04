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

public class DrillthroughTest extends TestBase {

  @Test
  public void testDrillOnSnowflakeSchema() throws Exception {
    String query =
      "DRILLTHROUGH\n"
      + "SELECT {[Product].[Product Subcategory].[Beer]} ON 0\n"
      + "FROM Sales\n";

    MondrianExpectation expectation = MondrianExpectation.newBuilder()
      .query( query )
      .expectResultSet()
      .rows(
        "Drink|Alcoholic Beverages|Beer and Wine|Beer|Good|Good Imported Beer|1",
        "Drink|Alcoholic Beverages|Beer and Wine|Beer|Good|Good Imported Beer|3" )
      .partial()
      .sql(
        "select\n"
        + "    product_class.product_family as Product Family,\n"
        + "    product_class.product_department as Product Department,\n"
        + "    product_class.product_category as Product Category,\n"
        + "    product_class.product_subcategory as Product Subcategory,\n"
        + "    product.brand_name as Brand Name,\n"
        + "    product.product_name as Product Name,\n"
        + "    sales_fact_1997.unit_sales as Unit Sales\n"
        + "from\n"
        + "    product_class as product_class,\n"
        + "    product as product,\n"
        + "    sales_fact_1997 as sales_fact_1997\n"
        + "where\n"
        + "    sales_fact_1997.product_id = product.product_id\n"
        + "and\n"
        + "    product.product_class_id = product_class.product_class_id\n"
        + "and\n"
        + "    product_class.product_family = 'Drink'\n"
        + "and\n"
        + "    product_class.product_department = 'Alcoholic Beverages'\n"
        + "and\n"
        + "    product_class.product_category = 'Beer and Wine'\n"
        + "and\n"
        + "    product_class.product_subcategory = 'Beer'\n" )
      .build();
    MondrianContext.forCatalog( FoodMartCatalogs.SNOWFLAKE_WITH_PRODUCT ).verify( expectation );
  }

  @Test
  public void testDrillOnSnowflakeSchemaNoSpaces() throws Exception {
    String query =
      "DRILLTHROUGH\n"
      + "SELECT {[Product].[ProductSubcategory].[Beer]} ON 0\n"
      + "FROM Sales\n";

    MondrianExpectation expectation = MondrianExpectation.newBuilder()
      .query( query )
      .expectResultSet()
      .rows(
        "Drink|Alcoholic Beverages|Beer and Wine|Beer|Good|Good Imported Beer|1",
        "Drink|Alcoholic Beverages|Beer and Wine|Beer|Good|Good Imported Beer|3" )
      .partial()
      .sql(
        "select\n"
        + "    product_class.product_family as ProductFamily,\n"
        + "    product_class.product_department as ProductDepartment,\n"
        + "    product_class.product_category as ProductCategory,\n"
        + "    product_class.product_subcategory as ProductSubcategory,\n"
        + "    product.brand_name as BrandName,\n"
        + "    product.product_name as ProductName,\n"
        + "    sales_fact_1997.unit_sales as UnitSales\n"
        + "from\n"
        + "    product_class as product_class,\n"
        + "    product as product,\n"
        + "    sales_fact_1997 as sales_fact_1997\n"
        + "where\n"
        + "    sales_fact_1997.product_id = product.product_id\n"
        + "and\n"
        + "    product.product_class_id = product_class.product_class_id\n"
        + "and\n"
        + "    product_class.product_family = 'Drink'\n"
        + "and\n"
        + "    product_class.product_department = 'Alcoholic Beverages'\n"
        + "and\n"
        + "    product_class.product_category = 'Beer and Wine'\n"
        + "and\n"
        + "    product_class.product_subcategory = 'Beer'\n" )
      .build();
    MondrianContext.forCatalog( FoodMartCatalogs.SNOWFLAKE_WITH_PRODUCT_NO_SPACES ).verify( expectation );
  }

  @Test
  public void testDrillOnDegenerateSchema() throws Exception {
    String query =
      "DRILLTHROUGH\n"
      + "SELECT {[customer].[customer id].[5]} ON 0\n"
      + "FROM Sales\n";

    MondrianExpectation expectation = MondrianExpectation.newBuilder()
      .query( query )
      .expectResultSet()
      .rows( "5|2" )
      .sql(
        "select\n"
        + "    sales_fact_1997.customer_id as customer id,\n"
        + "    sales_fact_1997.unit_sales as Unit Sales\n"
        + "from\n"
        + "    sales_fact_1997 as sales_fact_1997\n"
        + "where\n"
        + "    sales_fact_1997.customer_id = 5\n"
        + "order by\n"
        + "    sales_fact_1997.customer_id ASC" )
      .build();
    MondrianContext.forCatalog( FoodMartCatalogs.FLAT_WITH_CUSTOMER ).verify( expectation );
  }

  @Test
  public void testDrillOnDegenerateSchemaNoSpaces() throws Exception {
    String query =
      "DRILLTHROUGH\n"
      + "SELECT {[customer].[customerid].[5]} ON 0\n"
      + "FROM Sales\n";

    MondrianExpectation expectation = MondrianExpectation.newBuilder()
      .query( query )
      .expectResultSet()
      .rows( "5|2" )
      .sql(
        "select\n"
        + "    sales_fact_1997.customer_id as customerid,\n"
        + "    sales_fact_1997.unit_sales as UnitSales\n"
        + "from\n"
        + "    sales_fact_1997 as sales_fact_1997\n"
        + "where\n"
        + "    sales_fact_1997.customer_id = 5\n"
        + "order by\n"
        + "    "
        + getOrderExpression(
            "customerid",
            "sales_fact_1997.customer_id",
            true, true, false ) )
      .build();
    MondrianContext.forCatalog( FoodMartCatalogs.FLAT_WITH_CUSTOMER_NO_SPACES ).verify( expectation );
  }
}
