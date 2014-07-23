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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pentaho.mondrian.tck.MondrianExpectation.newBuilder;

@SuppressWarnings( "UnusedDeclaration" )
public class ExampleTest {
  private static final Logger logger = LoggerFactory.getLogger( ExampleTest.class );

  @SuppressWarnings( "UnusedDeclaration" )
  public void testExampleSelectFromSales() throws Exception {
    final MondrianExpectation expectation =
      newBuilder()
        .query( "select from sales" )
        .result(
          "Axis #0:\n"
            + "{}\n"
            + "266,773" )
        .sql( "select\n"
          + "    time_by_day.the_year as c0,\n"
          + "    sum(sales_fact_1997.unit_sales) as m0\n"
          + "from\n"
          + "    time_by_day time_by_day,\n"
          + "    sales_fact_1997 sales_fact_1997\n"
          + "where\n"
          + "    sales_fact_1997.time_id = time_by_day.time_id\n"
          + "and\n"
          + "    time_by_day.the_year = 1997\n"
          + "group by\n"
          + "    time_by_day.the_year" )
        .build();
    MondrianContext context = MondrianContext.defaultContext();
    context.verify( expectation );
  }

  @SuppressWarnings( "UnusedDeclaration" )
  public void testExampleOverrideProperties() throws Exception {
    new PropertyContext()
      .withProperty( MondrianProperties.instance().ResultLimit, "5" )
      .execute(
        new Runnable() {
          @Override
          public void run() {
            MondrianExpectation expectation =
              MondrianExpectation.newBuilder().query( "Select from Sales" ).build();
            try {
              MondrianContext.defaultContext().verify( expectation );
            } catch ( Exception e ) {
              logger.error( "oops", e );
              throw new RuntimeException( e );
            }
          }
        } );
  }
}
