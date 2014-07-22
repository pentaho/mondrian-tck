package org.pentaho.mondrian.tck;

import org.junit.Test;

public class TopCountTest {
  @Test
  public void testTopCount() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
      .query( "select non empty TopCount([Store].[Store].[Store Name].members,5,[Measures].[Unit Sales]) on 0, [Measures].[Unit Sales] on 1 from Sales" )
      .result( "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Store].[USA].[OR].[Salem].[Store 13]}\n"
        + "{[Store].[USA].[WA].[Tacoma].[Store 17]}\n"
        + "{[Store].[USA].[OR].[Portland].[Store 11]}\n"
        + "{[Store].[USA].[CA].[Los Angeles].[Store 7]}\n"
        + "{[Store].[USA].[CA].[San Diego].[Store 24]}\n"
        + "Axis #2:\n"
        + "{[Measures].[Unit Sales]}\n"
        + "Row #0: 41,580\n"
        + "Row #0: 35,257\n"
        + "Row #0: 26,079\n"
        + "Row #0: 25,663\n"
        + "Row #0: 25,635\n" )
      .sql(
        "select\n"
          + "    store.store_country as c0,\n"
          + "    store.store_state as c1,\n"
          + "    store.store_city as c2,\n"
          + "    store.store_name as c3,\n"
          + "    sum(sales_fact_1997.unit_sales) as c4\n"
          + "from\n"
          + "    store store,\n"
          + "    sales_fact_1997 sales_fact_1997\n"
          + "where\n"
          + "    sales_fact_1997.store_id = store.store_id\n"
          + "group by\n"
          + "    store.store_country,\n"
          + "    store.store_state,\n"
          + "    store.store_city,\n"
          + "    store.store_name\n"
          + "order by\n"
          + "    CASE WHEN sum(sales_fact_1997.unit_sales) IS NULL THEN 1 ELSE 0 END, sum(sales_fact_1997.unit_sales) DESC,\n"
          + "    CASE WHEN store.store_country IS NULL THEN 1 ELSE 0 END, store.store_country ASC,\n"
          + "    CASE WHEN store.store_state IS NULL THEN 1 ELSE 0 END, store.store_state ASC,\n"
          + "    CASE WHEN store.store_city IS NULL THEN 1 ELSE 0 END, store.store_city ASC,\n"
          + "    CASE WHEN store.store_name IS NULL THEN 1 ELSE 0 END, store.store_name ASC"
      )
      .build();
    MondrianContext.defaultContext().withCatalog(
      "<Schema name=\"FoodMart\">"
      + "  <Dimension name=\"Store\">\n"
      + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
      + "      <Table name=\"store\"/>\n"
      + "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
      + "      <Level name=\"Store State\" column=\"store_state\" uniqueMembers=\"true\"/>\n"
      + "      <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\"/>\n"
      + "      <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\">\n"
      + "      </Level>\n"
      + "    </Hierarchy>\n"
      + "  </Dimension>"
      + "  <Cube name=\"Sales\" defaultMeasure=\"Unit Sales\">"
      + "    <Table name=\"sales_fact_1997\"/>"
      + "    <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>"
      + "    <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
      + "      formatString=\"Standard\"/>"
      + "  </Cube>"
      + "</Schema>" ).verify( expectation );
  }
}
