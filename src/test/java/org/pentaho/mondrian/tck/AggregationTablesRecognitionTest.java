/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2013 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/

package org.pentaho.mondrian.tck;

import mondrian.olap.MondrianProperties;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.pentaho.mondrian.tck.MondrianExpectation.newBuilder;

public class AggregationTablesRecognitionTest {
  @Test
  public void testGetTablesJdbc() throws Exception {
    final SqlExpectation expectation =
        SqlExpectation.newBuilder()
            .query(
                new SqlExpectation.ResultSetProvider() {
                  public ResultSet getData( Connection conn, Statement statement ) throws Exception {
                    return conn.getMetaData()
                        .getTables( conn.getCatalog(), "", "%", new String[]{"TABLE", "VIEW"} );
                  }
                } )

            // we expect at least these columns, different drivers may produce more columns
            .columns( "table_cat", "table_schem", "table_name", "table_type" )
            .columnsPartial()
            .rows(
                "foodmart|null|account|TABLE",
                "foodmart|null|agg_10_foo_fact|TABLE",
                "foodmart|null|agg_c_10_sales_fact_1997|TABLE",
                "foodmart|null|agg_c_14_sales_fact_1997|TABLE",
                "foodmart|null|agg_c_special_sales_fact_1997|TABLE",
                "foodmart|null|agg_g_ms_pcat_sales_fact_1997|TABLE",
                "foodmart|null|agg_l_03_sales_fact_1997|TABLE",
                "foodmart|null|agg_l_04_sales_fact_1997|TABLE",
                "foodmart|null|agg_l_05_sales_fact_1997|TABLE",
                "foodmart|null|agg_lc_06_sales_fact_1997|TABLE",
                "foodmart|null|agg_lc_100_sales_fact_1997|TABLE",
                "foodmart|null|agg_line_class|TABLE",
                "foodmart|null|agg_ll_01_sales_fact_1997|TABLE",
                "foodmart|null|agg_pl_01_sales_fact_1997|TABLE" )
            .partial()
            .build();

    // Validate
    SqlContext.defaultContext().verify( expectation );

  }

  @Test
  public void testAggregationRecognition() {
    new PropertyContext()
        .withProperty( MondrianProperties.instance().UseAggregates, "true" )
        .withProperty( MondrianProperties.instance().ReadAggregates, "true" )
        .execute( new Runnable() {
          @Override
          public void run() {
            MondrianExpectation expectation = newBuilder()
                .query( "SELECT [Measures].[customer_count] on 0 from [Sales]" )
                .sql( "select\n"
                    + "    sum(agg_c_10_sales_fact_1997.customer_count) as m0\n"
                    + "from\n"
                    + "    agg_c_10_sales_fact_1997 as agg_c_10_sales_fact_1997" )
                .build();

            try {
              MondrianContext.forCatalog( FoodMartCatalogs.FLAT_WITH_CUSTOMER.replace(
                  "<Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>",
                  "<Measure name=\"customer_count\" column=\"customer_id\" aggregator=\"count\"/>\n"
                  + "<Measure name=\"store_cost\" column=\"store_cost\" aggregator=\"max\"/>" ) )
                  .verify( expectation );
            } catch ( Exception e ) {
              throw new RuntimeException( e );
            }
          }
        } );
  }
}
