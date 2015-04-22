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

public class FoodMartCatalogs {
  public static final String FLAT_WITH_CUSTOMER =
      "<Schema name=\"FoodMart\">"
      + "  <Cube name=\"Sales\" defaultMeasure=\"Unit Sales\">"
      + "    <Table name=\"sales_fact_1997\"/>"
      + "  <Dimension name=\"customer\">\n"
      + "    <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
      + "      <Table name=\"sales_fact_1997\"/>\n"
      + "      <Level name=\"customer id\" type=\"Integer\" internalType=\"int\" column=\"customer_id\" uniqueMembers=\"true\"/>\n"
      + "    </Hierarchy>\n"
      + "  </Dimension>"
      + "    <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>"
      + "  </Cube>"
      + "</Schema>";
  public static final String FLAT_WITH_CUSTOMER_NO_SPACES =
      "<Schema name=\"FoodMart\">"
      + "  <Cube name=\"Sales\" defaultMeasure=\"UnitSales\">"
      + "    <Table name=\"sales_fact_1997\"/>"
      + "  <Dimension name=\"customer\">\n"
      + "    <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
      + "      <Table name=\"sales_fact_1997\"/>\n"
      + "      <Level name=\"customerid\" type=\"Integer\" internalType=\"int\" column=\"customer_id\" uniqueMembers=\"true\"/>\n"
      + "    </Hierarchy>\n"
      + "  </Dimension>"
      + "    <Measure name=\"UnitSales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>"
      + "  </Cube>"
      + "</Schema>";
  public static final String FLAT_WITH_FEW_DIMS =
      "<Schema name=\"FoodMart\">"
      + "  <Cube name=\"Sales\" defaultMeasure=\"Unit Sales\">"
      + "    <Table name=\"sales_fact_1997\"/>"
      + "  <Dimension name=\"customer\">\n"
      + "    <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
      + "      <Table name=\"sales_fact_1997\"/>\n"
      + "      <Level name=\"customer id\" type=\"Integer\" column=\"customer_id\" uniqueMembers=\"true\"/>\n"
      + "    </Hierarchy>\n"
      + "  </Dimension>"
      + "  <Dimension name=\"store\">\n"
      + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
      + "      <Table name=\"sales_fact_1997\"/>\n"
      + "      <Level name=\"store id\" type=\"Integer\" column=\"store_id\" uniqueMembers=\"true\"/>\n"
      + "    </Hierarchy>\n"
      + "  </Dimension>"
      + "    <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>"
      + "  </Cube>"
      + "</Schema>";
  public static final String STAR_WITH_STORE =
      "<Schema name=\"FoodMart\">"
      + "  <Cube name=\"Sales\" defaultMeasure=\"Unit Sales\">"
      + "    <Table name=\"sales_fact_1997\"/>"
      + "    <Dimension name=\"Store\" foreignKey=\"store_id\">"
      + "      <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">"
      + "        <Table name=\"store\"/>"
      + "        <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>"
      + "        <Level name=\"Store State\" column=\"store_state\" uniqueMembers=\"true\"/>"
      + "        <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\"/>"
      + "        <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\"/>"
      + "      </Hierarchy>"
      + "    </Dimension>"
      + "    <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>"
      + "<Measure name=\"Customer Count\" column=\"customer_id\" aggregator=\"distinct-count\" formatString=\"#,###\"/>"
      + "  </Cube>"
      + "</Schema>";
  public static final String SNOWFLAKE_WITH_PRODUCT =
      "<Schema name=\"FoodMart\">"
      + "  <Cube name=\"Sales\" defaultMeasure=\"Unit Sales\">"
      + "    <Table name=\"sales_fact_1997\"/>"
      + "    <Dimension name=\"Product\" foreignKey=\"product_id\">\n"
      + "      <Hierarchy hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
      + "        <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
      + "          <Table name=\"product\"/>\n"
      + "          <Table name=\"product_class\"/>\n"
      + "        </Join>\n"
      + "        <Level name=\"Product Family\" table=\"product_class\" column=\"product_family\"\n"
      + "          uniqueMembers=\"true\"/>\n"
      + "        <Level name=\"Product Department\" table=\"product_class\" column=\"product_department\"\n"
      + "          uniqueMembers=\"false\"/>\n"
      + "        <Level name=\"Product Category\" table=\"product_class\" column=\"product_category\"\n"
      + "          uniqueMembers=\"false\"/>\n"
      + "        <Level name=\"Product Subcategory\" table=\"product_class\" column=\"product_subcategory\"\n"
      + "          uniqueMembers=\"false\"/>\n"
      + "        <Level name=\"Brand Name\" table=\"product\" column=\"brand_name\" uniqueMembers=\"false\"/>\n"
      + "        <Level name=\"Product Name\" table=\"product\" column=\"product_name\"\n"
      + "          uniqueMembers=\"true\"/>\n"
      + "      </Hierarchy>\n"
      + "    </Dimension>"
      + "    <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>"
      + "  </Cube>"
      + "</Schema>";
  public static final String SNOWFLAKE_WITH_PRODUCT_NO_SPACES =
      "<Schema name=\"FoodMart\">"
      + "  <Cube name=\"Sales\" defaultMeasure=\"UnitSales\">"
      + "    <Table name=\"sales_fact_1997\"/>"
      + "    <Dimension name=\"Product\" foreignKey=\"product_id\">\n"
      + "      <Hierarchy hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
      + "        <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
      + "          <Table name=\"product\"/>\n"
      + "          <Table name=\"product_class\"/>\n"
      + "        </Join>\n"
      + "        <Level name=\"ProductFamily\" table=\"product_class\" column=\"product_family\"\n"
      + "          uniqueMembers=\"true\"/>\n"
      + "        <Level name=\"ProductDepartment\" table=\"product_class\" column=\"product_department\"\n"
      + "          uniqueMembers=\"false\"/>\n"
      + "        <Level name=\"ProductCategory\" table=\"product_class\" column=\"product_category\"\n"
      + "          uniqueMembers=\"false\"/>\n"
      + "        <Level name=\"ProductSubcategory\" table=\"product_class\" column=\"product_subcategory\"\n"
      + "          uniqueMembers=\"false\"/>\n"
      + "        <Level name=\"BrandName\" table=\"product\" column=\"brand_name\" uniqueMembers=\"false\"/>\n"
      + "        <Level name=\"ProductName\" table=\"product\" column=\"product_name\"\n"
      + "          uniqueMembers=\"true\"/>\n"
      + "      </Hierarchy>\n"
      + "    </Dimension>"
      + "    <Measure name=\"UnitSales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>"
      + "  </Cube>"
      + "</Schema>";
}
