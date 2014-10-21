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
