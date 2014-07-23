package org.pentaho.mondrian.tck;

import com.google.common.base.Function;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

public class TopCountTest {
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
      .rows( "6.0", "6.0" )
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
          + "order by\n"
          + "    CASE WHEN sum(sales_fact_1997.unit_sales) IS NULL THEN 1 ELSE 0 END, sum(sales_fact_1997.unit_sales) DESC,\n"
          + "    CASE WHEN sales_fact_1997.customer_id IS NULL THEN 1 ELSE 0 END, sales_fact_1997.customer_id ASC" )
      .build();
    MondrianContext.defaultContext().withCatalog(
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
        + "</Schema>" ).verify( expectation );
  }
}
