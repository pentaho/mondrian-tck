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
          + "and wc2.warehouse_class_id = wc2.warehouse_class_id\n"
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
}
