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

public class FoodMartCatalogs {
  public static final String FLAT_WITH_CUSTOMER =
    "<Schema name=\"FoodMart\">"
    + "  <Cube name=\"Sales\" defaultMeasure=\"Unit Sales\">"
    + "    <Table name=\"sales_fact_1997\"/>"
    + "  <Dimension name=\"customer\">\n"
    + "    <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
    + "      <Table name=\"sales_fact_1997\"/>\n"
    + "      <Level name=\"customer id\" type=\"Integer\" column=\"customer_id\" uniqueMembers=\"true\"/>\n"
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
}
