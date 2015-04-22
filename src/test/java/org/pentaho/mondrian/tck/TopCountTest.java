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

import com.google.common.base.Function;

import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

public class TopCountTest extends TestBase {

  @Test
  public void testSetMaxRows() throws Exception {
    SqlContext sqlContext = SqlContext.defaultContext();
    SqlExpectation sqlExpectation = SqlExpectation.newBuilder()
        .query( "select unit_sales from sales_fact_1997 order by unit_sales desc" )
        .modifyStatement( new Function<Statement, Void>() {
          @Override
          public Void apply( final Statement statement ) {
            try {
              statement.setMaxRows( 2 );
            } catch ( SQLException e ) {
              fail( "Should have been able to setMaxRows" );
            }
            return null;
          }
        } )
        .rows( "6", "6" )
        .build();
    sqlContext.verify( sqlExpectation );
  }

  @Test
  public void testTopCount() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
        .query(
          "select "
            + "  non empty TopCount([customer].[customer].[customer id].members,5,[Measures].[Unit Sales]) on 0,"
            + "  [Measures].[Unit Sales] on 1 "
            + "  from Sales" )
        .result(
          "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[customer].[4021]}\n"
            + "{[customer].[8452]}\n"
            + "{[customer].[5295]}\n"
            + "{[customer].[4727]}\n"
            + "{[customer].[1297]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 518\n"
            + "Row #0: 447\n"
            + "Row #0: 441\n"
            + "Row #0: 439\n"
            + "Row #0: 392\n" )
        .sql(
          "select\n"
            + "    sales_fact_1997.customer_id as c0,\n"
            + "    sum(sales_fact_1997.unit_sales) as c1\n"
            + "from\n"
            + "    sales_fact_1997 sales_fact_1997\n"
            + "group by\n"
            + "    sales_fact_1997.customer_id\n"
            + "order by"
            + "\n    " + getOrderExpression( "c0", "sum(sales_fact_1997.unit_sales)", true, false, true )
            + ",\n    " + getOrderExpression( "c1", "sales_fact_1997.customer_id", true, true, true ) )
        .build();
    MondrianContext.forCatalog( FoodMartCatalogs.FLAT_WITH_CUSTOMER ).verify( expectation );
  }
}
