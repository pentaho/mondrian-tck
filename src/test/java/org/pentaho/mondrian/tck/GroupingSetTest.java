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
        String query = getGSetSQLQuery("customer.gender as gender, sum(sales_fact_1997.store_cost) as sum_cost"
                ,"(customer.gender)");
        SqlExpectation expectation = SqlExpectation.newBuilder()
                .query( query )
                .columns("gender","sum_cost")
                .rows( "M|113849.7546000008", "F|111777.47900000079" )
                .build();
        SqlContext.defaultContext().verify( expectation );
    }

    /**
     * Test "grouping sets ((col1,col2))"
     */
    @Test
    public void testComplexEntry() throws Exception {
        String query = getGSetSQLQuery("time_by_day.the_year as the_year, customer.gender as gender, sum(sales_fact_1997.store_cost) as sum_cost"
                ,"(time_by_day.the_year, customer.gender)");
        SqlExpectation expectation = SqlExpectation.newBuilder()
                .query( query )
                .columns("the_year","gender","sum_cost")
                .rows( "1997|M|113849.7546000008", "1997|F|111777.47900000079" )
                .build();
        SqlContext.defaultContext().verify( expectation );
    }

    /**
     * Test "grouping sets ((col1), ())"
     */
    @Test
    public void testEmptyEntry() throws Exception {
        String query = getGSetSQLQuery("customer.gender as gender, sum(sales_fact_1997.store_cost) as sum_cost"
                ,"(customer.gender),()");
        SqlExpectation expectation = SqlExpectation.newBuilder()
                .query(query)
                .columns("gender", "sum_cost")
                .rows("M|113849.7546000008", "F|111777.47900000079", "null|225627.2336000015")
                .build();
        SqlContext.defaultContext().verify( expectation );
    }

    /**
     * Test "grouping sets ((col1,col2),(col3))"
     */
    @Test
    public void testMultipleEntries() throws Exception {
        String query = getGSetSQLQuery("time_by_day.the_year as the_year, customer.gender as gender, sum(sales_fact_1997.store_cost) as sum_cost"
                ,"(time_by_day.the_year, customer.gender), (time_by_day.the_year),()");
        SqlExpectation expectation = SqlExpectation.newBuilder()
                .query( query )
                .columns("the_year","gender","sum_cost")
                .rows( "1997|M|113849.7546000008", "1997|F|111777.47900000079", "null|225627.2336000015", "null|225627.2336000015" )
                .build();
        SqlContext.defaultContext().verify( expectation );
    }



    private String getGSetSQLQuery(String columns, String conditions) {
        return  "select " + columns + "\n" +
                "from time_by_day, sales_fact_1997, customer \n" +
                "where (sales_fact_1997.time_id = time_by_day.time_id and time_by_day.the_year = 1997\n" +
                "and sales_fact_1997.customer_id = customer.customer_id)\n" +
                "group by grouping sets (" + conditions + ")";
    }

    @Test
    public void testGSetMondrian() throws Exception {
        MondrianExpectation expectation = MondrianExpectation.newBuilder()
                .query(MDX)
                .result("Axis #0:\n" +
                            "{}\n" +
                        "Axis #1:\n" +
                            "{[Measures].[Store Cost]}\n" +
                        "Axis #2:\n" +
                            "{[Gender].[F]}\n" +
                            "{[Gender].[M]}\n" +
                            "{[Gender].[agg]}\n" +
                            "Row #0: 111,777.48\n" +
                            "Row #1: 113,849.75\n" +
                            "Row #2: 225,627.23\n")
                .build();
        MondrianContext.defaultContext().verify(expectation);
    }

}
