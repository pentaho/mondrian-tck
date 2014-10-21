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

import static org.pentaho.mondrian.tck.SqlExpectation.newBuilder;

import java.sql.Types;

import org.junit.Test;

public class JoinTest {

  /**
   * This test verifies that we can join two tables through the WHERE clause.
   *
   *    Fact----->D1
   */
  @Test
  public void testOneWayJoin89() throws Exception {
    final SqlExpectation expct =
      newBuilder()

        // We join two tables in the WHERE clause and select a column in each
        .query( "select warehouse.warehouse_id, warehouse_class.description from warehouse, warehouse_class where warehouse.warehouse_class_id = warehouse_class.warehouse_class_id" )

        // We expect two columns.
        .columns( "warehouse_id", "description" )

        // We want an integer and a string
        .types( Types.INTEGER, Types.VARCHAR )

        // We will validate the first rows content.
        .rows( "1|Small Independent" )

        // We won't validate all rows.
        .partial()
        .build();

    // Validate
    SqlContext.defaultContext().verify( expct );
  }

  /**
   * This test verifies that we can join two tables through the FROM clause.
   *
   *    Fact----->D1
   */
  @Test
  public void testOneWayJoin92() throws Exception {
    final SqlExpectation expct =
      newBuilder()

        // We join two tables in the FROM clause and select a column in each
        .query( "select warehouse.warehouse_id, warehouse_class.description from warehouse join warehouse_class on (warehouse.warehouse_class_id = warehouse_class.warehouse_class_id)" )

        // We expect two columns.
        .columns( "warehouse_id", "description" )

        // We want an integer and a string
        .types( Types.INTEGER, Types.VARCHAR )

        // We will validate the first rows content.
        .rows( "1|Small Independent" )

        // We won't validate all rows.
        .partial()
        .build();

    // Validate
    SqlContext.defaultContext().verify( expct );
  }

  /**
   * This test verifies that we can create a cross product of two tables
   * by omitting any join or equality predicates.
   *
   *     D1-----D2
   */
  @Test
  public void testImplicitJoin() throws Exception {
    final SqlExpectation expct =
      newBuilder()

        // We put two tables in the FROM clause and we want the cross product of both sets of rows.
        .query( "select warehouse.warehouse_id, warehouse_class.description from warehouse, warehouse_class" )

        // We expect two columns.
        .columns( "warehouse_id", "description" )

        // We want an integer and a string
        .types( Types.INTEGER, Types.VARCHAR )

        // We will validate the first rows content.
        .rows( "1|Small Independent" )

        // We won't validate all rows.
        .partial()
        .build();

    // Validate
    SqlContext.defaultContext().verify( expct );
  }

  /**
   * This test verifies that we can join three tables together by using SQL-89 style joins.
   *
   *     D1<-----Fact----->D2
   */
  @Test
  public void testTwoWayJoin89() throws Exception {
    final SqlExpectation expct =
      newBuilder()

        .query( "select warehouse.warehouse_id, warehouse_class.description, inventory_fact_1997.store_id from warehouse, warehouse_class, inventory_fact_1997 where warehouse.warehouse_class_id = warehouse_class.warehouse_class_id and inventory_fact_1997.warehouse_id = warehouse.warehouse_id" )

        .columns( "warehouse_id", "description", "store_id" )

        .types( Types.INTEGER, Types.VARCHAR, Types.INTEGER )

        .rows( "2|Medium Independent|2" )

        .partial()
        .build();

    SqlContext.defaultContext().verify( expct );
  }

  /**
   * This test verifies that we can join three tables together by using SQL-92 style joins.
   *
   *     D1<-----Fact----->D2
   */
  @Test
  public void testTwoWayJoin92() throws Exception {
    final SqlExpectation expct =
      newBuilder()

        .query( "select warehouse.warehouse_id, warehouse_class.description, inventory_fact_1997.store_id from warehouse join warehouse_class on (warehouse.warehouse_class_id = warehouse_class.warehouse_class_id) join inventory_fact_1997 on (inventory_fact_1997.warehouse_id = warehouse.warehouse_id)" )

        .columns( "warehouse_id", "description", "store_id" )

        .types( Types.INTEGER, Types.VARCHAR, Types.INTEGER )

        .rows( "2|Medium Independent|2" )

        .partial()
        .build();

    SqlContext.defaultContext().verify( expct );
  }

  /**
   * This test verifies that we can do a 3 way join, SQL-89 style.
   * This results in a fact table being joined by 3 different tables and exercises
   * the optimizers capability to handle star schemas.
   *
   *     D1<--------      ----->D2
   *                \    /
   *                 Fact
   *     D3 <-------/
   */
  @Test
  public void testThreeWayJoin89() throws Exception {
    final SqlExpectation expct =
      newBuilder()

        .query( "select warehouse.warehouse_id, inventory_fact_1997.store_id, time_by_day.time_id, product.product_id\n"
          + "from warehouse, inventory_fact_1997, product, time_by_day\n"
          + "where inventory_fact_1997.warehouse_id = warehouse.warehouse_id and inventory_fact_1997.time_id = time_by_day.time_id and inventory_fact_1997.product_id = product.product_id\n"
          + "order by warehouse.warehouse_id, inventory_fact_1997.store_id, time_by_day.time_id, product.product_id" )

        .columns( "warehouse_id", "store_id", "time_id", "product_id" )

        .types( Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER )

        .rows( "2|2|369|94" )

        .partial()
        .build();

    SqlContext.defaultContext().verify( expct );
  }

  /**
   * This test verifies that we can do a 3 way join, SQL-92 style.
   * This results in a fact table being joined by 3 different tables and exercises
   * the optimizers capability to handle star schemas.
   *
   *     D1<--------      ----->D2
   *                \    /
   *                 Fact
   *     D3 <-------/
   */
  @Test
  public void testThreeWayJoin92() throws Exception {
    final SqlExpectation expct =
      newBuilder()

        .query( "select warehouse.warehouse_id, inventory_fact_1997.store_id, time_by_day.time_id, product.product_id\n"
          + "from inventory_fact_1997\n"
          + "join warehouse on (inventory_fact_1997.warehouse_id = warehouse.warehouse_id)\n"
          + "join product on (inventory_fact_1997.product_id = product.product_id)\n"
          + "join time_by_day on (inventory_fact_1997.time_id = time_by_day.time_id)\n"
          + "order by warehouse.warehouse_id, inventory_fact_1997.store_id, time_by_day.time_id, product.product_id" )

        .columns( "warehouse_id", "store_id", "time_id", "product_id" )

        .types( Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER )

        .rows( "2|2|369|94" )

        .partial()
        .build();

    SqlContext.defaultContext().verify( expct );
  }

  /**
   * This test verifies that we can do a 3 way join, SQL-89 style.
   * We also add one more table joined as a leaf to one of the dimension table.
   * This results in a fact table being joined by 3 different tables and exercises
   * the optimizers capability to handle star schemas that use multiple tables to represent
   * a dimension.
   *
   *     D1'<--D1<--      ----->D2
   *                \    /
   *                 Fact
   *     D3 <-------/
   */
  @Test
  public void testThreeWayJoinPlusSingleStarLeaf89() throws Exception {
    final SqlExpectation expct =
      newBuilder()

        .query( "select warehouse_class.description, warehouse.warehouse_id, inventory_fact_1997.store_id, time_by_day.time_id, product.product_id\n"
          + "from warehouse, inventory_fact_1997, product, time_by_day, warehouse_class\n"
          + "where inventory_fact_1997.warehouse_id = warehouse.warehouse_id and inventory_fact_1997.time_id = time_by_day.time_id and inventory_fact_1997.product_id = product.product_id and warehouse_class.warehouse_class_id = warehouse.warehouse_class_id\n"
          + "order by warehouse.warehouse_id, inventory_fact_1997.store_id, time_by_day.time_id, product.product_id" )

        .columns( "description", "warehouse_id", "store_id", "time_id", "product_id" )

        .types( Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER )

        .rows( "Medium Independent|2|2|369|94" )

        .partial()
        .build();

    SqlContext.defaultContext().verify( expct );
  }

  /**
   * This test verifies that we can do a 3 way join, SQL-92 style.
   * This results in a fact table being joined by 3 different tables and exercises
   * the optimizers capability to handle star schemas.
   *
   *     D1'<--D1<--      ----->D2
   *                \    /
   *                 Fact
   *     D3 <-------/
   */
  @Test
  public void testThreeWayJoinPlusSingleStarLeaf92() throws Exception {
    final SqlExpectation expct =
      newBuilder()

        .query( "select warehouse_class.description, warehouse.warehouse_id, inventory_fact_1997.store_id, time_by_day.time_id, product.product_id\n"
          + "from inventory_fact_1997\n"
          + "join warehouse on (inventory_fact_1997.warehouse_id = warehouse.warehouse_id)\n"
          + "join product on (inventory_fact_1997.product_id = product.product_id)\n"
          + "join time_by_day on (inventory_fact_1997.time_id = time_by_day.time_id)\n"
          + "join warehouse_class on (warehouse_class.warehouse_class_id = warehouse.warehouse_class_id)\n"
          + "order by warehouse.warehouse_id, inventory_fact_1997.store_id, time_by_day.time_id, product.product_id" )

        .columns( "description", "warehouse_id", "store_id", "time_id", "product_id" )

        .types( Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER )

        .rows( "Medium Independent|2|2|369|94" )

        .partial()
        .build();

    SqlContext.defaultContext().verify( expct );
  }

  /**
   * This test verifies that we can do a 3 way join, SQL-89 style.
   * We also add two more tables joined as leafs to one of the dimension table.
   * This results in a fact table being joined by 3 different tables and exercises
   * the optimizers capability to handle full star schemas that use multiple tables to represent
   * a dimension, one of these tables being shared by two hierarchies and acting as a bridge table.
   *
   *    D1'<--
   *          \
   *    D1''<--D1<--      ----->D2
   *                \    /
   *                 Fact
   *     D3 <-------/
   */
  @Test
  public void testThreeWayJoinPlusDoubleStarLeaf89() throws Exception {
    final SqlExpectation expct =
      newBuilder()

        .query( "select warehouse_class.description, wc2.description, warehouse.warehouse_id, inventory_fact_1997.store_id, time_by_day.time_id, product.product_id\n"
          + "from warehouse, inventory_fact_1997, product, time_by_day, warehouse_class, warehouse_class as wc2\n"
          + "where inventory_fact_1997.warehouse_id = warehouse.warehouse_id\n"
          + "and inventory_fact_1997.time_id = time_by_day.time_id\n"
          + "and inventory_fact_1997.product_id = product.product_id\n"
          + "and warehouse_class.warehouse_class_id = warehouse.warehouse_class_id\n"
          + "and wc2.warehouse_class_id = warehouse.warehouse_class_id\n"
          + "order by warehouse.warehouse_id, inventory_fact_1997.store_id, time_by_day.time_id, product.product_id" )

        .columns( "description", "description", "warehouse_id", "store_id", "time_id", "product_id" )

        .types( Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER )

        .rows( "Medium Independent|Medium Independent|2|2|369|94" )

        .partial()
        .build();

    SqlContext.defaultContext().verify( expct );
  }

  /**
   * This test verifies that we can do a 3 way join, SQL-92 style.
   * This results in a fact table being joined by 3 different tables and exercises
   * the optimizers capability to handle star schemas.
   *
   *    D1'<--
   *          \
   *    D1''<--D1<--      ----->D2
   *                \    /
   *                 Fact
   *     D3 <-------/
   */
  @Test
  public void testThreeWayJoinPlusDoubleStarLeaf92() throws Exception {
    final SqlExpectation expct =
      newBuilder()

        .query( "select warehouse_class.description, wc2.description, warehouse.warehouse_id, inventory_fact_1997.store_id, time_by_day.time_id, product.product_id\n"
          + "from inventory_fact_1997\n"
          + "join warehouse on (inventory_fact_1997.warehouse_id = warehouse.warehouse_id)\n"
          + "join product on (inventory_fact_1997.product_id = product.product_id)\n"
          + "join time_by_day on (inventory_fact_1997.time_id = time_by_day.time_id)\n"
          + "join warehouse_class on (warehouse_class.warehouse_class_id = warehouse.warehouse_class_id)\n"
          + "join warehouse_class as wc2 on (wc2.warehouse_class_id = warehouse.warehouse_class_id)\n"
          + "order by warehouse.warehouse_id, inventory_fact_1997.store_id, time_by_day.time_id, product.product_id" )

        .columns( "description", "description", "warehouse_id", "store_id", "time_id", "product_id" )

        .types( Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER )

        .rows( "Medium Independent|Medium Independent|2|2|369|94" )

        .partial()
        .build();

    SqlContext.defaultContext().verify( expct );
  }

  /**
   * This test verifies that we can do native crossjoins on a degenerate fact
   * table. We expect Customer and Store to be crossjoined in the same SQL query.
   */
  @Test
  public void testNonEmptyCrossJoinDegenerate() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
      .query(
        "select "
        + " non empty { CrossJoin([customer].[customer].members, [store].[store].members ) } on 0"
        + " from [Sales]" )
      .sql(
          "select\n"
          + "    sales_fact_1997.customer_id as c0,\n"
          + "    sales_fact_1997.store_id as c1\n"
          + "from\n"
          + "    sales_fact_1997 as sales_fact_1997\n"
          + "group by\n"
          + "    sales_fact_1997.customer_id,\n"
          + "    sales_fact_1997.store_id\n" )
      .build();
    MondrianContext.forCatalog(
      "<Schema name=\"FoodMart\">"
      + "  <Cube name=\"Sales\" defaultMeasure=\"Unit Sales\">"
      + "    <Table name=\"sales_fact_1997\"/>"
      + "  <Dimension name=\"customer\">\n"
      + "    <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
      + "      <Table name=\"sales_fact_1997\"/>\n"
      + "      <Level name=\"customer\" type=\"Integer\" column=\"customer_id\" uniqueMembers=\"true\"/>\n"
      + "    </Hierarchy>\n"
      + "  </Dimension>"
      + "  <Dimension name=\"store\">\n"
      + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
      + "      <Table name=\"sales_fact_1997\"/>\n"
      + "      <Level name=\"store\" type=\"Integer\" column=\"store_id\" uniqueMembers=\"true\"/>\n"
      + "    </Hierarchy>\n"
      + "  </Dimension>"
      + "    <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>"
      + "  </Cube>"
      + "</Schema>" ).verify( expectation );
  }

  /**
   * This test verifies that we can do native crossjoins on a star schema.
   * We expect Customer and Store tables to be crossjoined in the same SQL query
   * through the fact table.
   */
  @Test
  public void testNonEmptyCrossJoinStar() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
      .query(
        "select "
        + " non empty { CrossJoin([customer].[customer].members, [store].[store].members ) } on 0"
        + " from [Sales]" )
      .sql(
          "select\n"
          + "    customer.customer_id as c0,\n"
          + "    store.store_id as c1\n"
          + "from\n"
          + "    customer as customer,\n"
          + "    sales_fact_1997 as sales_fact_1997,\n"
          + "    store as store\n"
          + "where\n"
          + "    sales_fact_1997.customer_id = customer.customer_id\n"
          + "and\n"
          + "    sales_fact_1997.store_id = store.store_id\n"
          + "group by\n"
          + "    customer.customer_id,\n"
          + "    store.store_id\n" )
      .build();
    MondrianContext.forCatalog(
      "<Schema name=\"FoodMart\">"
      + "  <Cube name=\"Sales\" defaultMeasure=\"Unit Sales\">"
      + "    <Table name=\"sales_fact_1997\"/>"
      + "  <Dimension name=\"customer\" foreignKey=\"customer_id\">\n"
      + "    <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
      + "      <Table name=\"customer\"/>\n"
      + "      <Level name=\"customer\" type=\"Integer\" column=\"customer_id\" uniqueMembers=\"true\"/>\n"
      + "    </Hierarchy>\n"
      + "  </Dimension>"
      + "  <Dimension name=\"store\" foreignKey=\"store_id\">\n"
      + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
      + "      <Table name=\"store\"/>\n"
      + "      <Level name=\"store\" type=\"Integer\" column=\"store_id\" uniqueMembers=\"true\"/>\n"
      + "    </Hierarchy>\n"
      + "  </Dimension>"
      + "    <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>"
      + "  </Cube>"
      + "</Schema>" ).verify( expectation );
  }

  /**
   * This test verifies that we can do native crossjoins on a snowflake schema.
   */
  @Test
  public void testNonEmptyCrossJoinSnowflake() throws Exception {
    MondrianExpectation expectation = MondrianExpectation.newBuilder()
      .query(
        "select "
        + " non empty { CrossJoin([product].[product family].members, [store].[store].members ) } on 0"
        + " from [Sales]" )
      .sql(
        "select\n"
        + "    product_class.product_family as c0,\n"
        + "    store.store_id as c1\n"
        + "from\n"
        + "    product as product,\n"
        + "    product_class as product_class,\n"
        + "    sales_fact_1997 as sales_fact_1997,\n"
        + "    store as store\n"
        + "where\n"
        + "    product.product_class_id = product_class.product_class_id\n"
        + "and\n"
        + "    sales_fact_1997.product_id = product.product_id\n"
        + "and\n"
        + "    sales_fact_1997.store_id = store.store_id\n"
        + "group by\n"
        + "    product_class.product_family,\n"
        + "    store.store_id\n" )
      .build();
    MondrianContext.forCatalog(
      "<Schema name=\"FoodMart\">"
      + "  <Cube name=\"Sales\" defaultMeasure=\"Unit Sales\">"
      + "  <Table name=\"sales_fact_1997\"/>"
      + "  <Dimension name=\"product\" foreignKey=\"product_id\">\n"
      + "    <Hierarchy hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
      + "      <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
      + "        <Table name=\"product\"/>\n"
      + "        <Table name=\"product_class\"/>\n"
      + "      </Join>\n"
      + "      <Level name=\"product family\" table=\"product_class\" column=\"product_family\" uniqueMembers=\"true\"/>\n"
      + "    </Hierarchy>\n"
      + "  </Dimension>\n"
      + "  <Dimension name=\"store\" foreignKey=\"store_id\">\n"
      + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
      + "      <Table name=\"store\"/>\n"
      + "      <Level name=\"store\" type=\"Integer\" column=\"store_id\" uniqueMembers=\"true\"/>\n"
      + "    </Hierarchy>\n"
      + "  </Dimension>"
      + "    <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>"
      + "  </Cube>"
      + "</Schema>" ).verify( expectation );
  }
}
