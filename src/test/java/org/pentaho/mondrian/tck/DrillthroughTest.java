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

public class DrillthroughTest {

    @Test
    public void testReturnOnStarSchema() throws Exception {
        String query = "DRILLTHROUGH\n"
                + "maxrows 3\n"
                + "SELECT {[Customers].[USA].[CA].[Berkeley]} ON 0,\n"
                + "{[Time].[1997]} ON 1\n"
                + "FROM Sales\n"
                + "RETURN [Measures].[Unit Sales]";

        MondrianExpectation expectation = MondrianExpectation.newBuilder()
                .query( query )
                .expectResultSet(true)
                .rows("1.0", "1.0", "2.0")
                .sql("select\n" +
                        "    time_by_day.the_year as c0,\n" +
                        "    customer.state_province as c1,\n" +
                        "    customer.city as c2,\n" +
                        "    sum(sales_fact_1997.unit_sales) as m0\n" +
                        "from\n" +
                        "    time_by_day time_by_day,\n" +
                        "    sales_fact_1997 sales_fact_1997,\n" +
                        "    customer customer\n" +
                        "where\n" +
                        "    sales_fact_1997.time_id = time_by_day.time_id\n" +
                        "and\n" +
                        "    time_by_day.the_year = 1997\n" +
                        "and\n" +
                        "    sales_fact_1997.customer_id = customer.customer_id\n" +
                        "and\n" +
                        "    customer.state_province = 'CA'\n" +
                        "and\n" +
                        "    customer.city = 'Berkeley'\n" +
                        "group by\n" +
                        "    time_by_day.the_year,\n" +
                        "    customer.state_province,\n" +
                        "    customer.city")
                .build();
        MondrianContext.defaultContext().verify(expectation);
    }

    @Test
    public void testFactTable() throws Exception {
        String catalog =
                "<Schema name=\"FoodMart\">\n" +
                "  <Cube name=\"Store\">\n" +
                "    <Table name=\"store\"/>\n" +
                "    <Dimension name=\"Store_Type\">\n" +
                "      <Hierarchy hasAll=\"true\">\n" +
                "        <Level name=\"Store_Type\" column=\"store_type\" uniqueMembers=\"true\"/>\n" +
                "      </Hierarchy>\n" +
                "    </Dimension>\n" +
                "    <Dimension name=\"Has_coffee_bar\">\n" +
                "      <Hierarchy hasAll=\"true\">\n" +
                "        <Level name=\"Has_coffee_bar\" column=\"coffee_bar\" uniqueMembers=\"true\"\n" +
                "            type=\"Boolean\"/>\n" +
                "      </Hierarchy>\n" +
                "    </Dimension>\n" +
                "    <Measure name=\"Store_Sqft\" column=\"store_sqft\" aggregator=\"sum\"\n" +
                "        formatString=\"#,###\"/>\n" +
                "    <Measure name=\"Grocery_Sqft\" column=\"grocery_sqft\" aggregator=\"sum\"\n" +
                "        formatString=\"#,###\"/>\n" +
                "  </Cube>\n" +
                "</Schema>";

        String query = "DRILLTHROUGH\n"
                + "maxrows 2\n"
                + "SELECT {[Store_Type].[Supermarket]} ON 0,\n"
                + "{[Has_coffee_bar]} ON 1\n"
                + "FROM Store\n";

        MondrianExpectation expectation = MondrianExpectation.newBuilder()
                .query( query )
                .expectResultSet(true)
                .columns("store_type","has_coffee_bar","store_sqft")
                .rows("Supermarket|null|null","Supermarket|null|39696")
                .build();
        MondrianContext.forCatalog(catalog).verify(expectation);
    }

    @Test
    // fails on Impala
    public void testFactTableWithSpaceInHierarchyName() throws Exception {
        String query = "DRILLTHROUGH\n"
                + "maxrows 2\n"
                + "SELECT {[Store Type].[Supermarket]} ON 0,\n"
                + "{[Has coffee bar]} ON 1\n"
                + "FROM Store\n";

        MondrianExpectation expectation = MondrianExpectation.newBuilder()
                .query( query )
                .expectResultSet(true)
                .columns("store_type","has_coffee_bar","store_sqft")
                .rows("Supermarket|null|null","Supermarket|null|39696")
                .partial()
                .build();
        MondrianContext.defaultContext().verify(expectation);
    }

}
