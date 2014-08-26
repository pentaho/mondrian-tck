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
