package org.pentaho.mondrian.tck;

import static org.pentaho.mondrian.tck.SqlExpectation.newBuilder;

import java.sql.Types;

import org.junit.Test;

public class JoinTest {

  /**
   * This test verifies that we can join two tables through the WHERE clause.
   */
  @Test
  public void testOneWayJoin89() throws Exception {
    final SqlExpectation expct =
      newBuilder()

        // We join two tables in the WHERE clause and select a column in each
        .query( "select warehouse.warehouse_id, warehouse_class.description from warehouse, warehouse_class where warehouse.warehouse_class_id = warehouse_class.warehouse_class_id" )

        // We expect two columns.
        .columns( new String[] { "warehouse_id", "description" } )

        // We want an integer and a string
        .types( new int[] { Types.INTEGER, Types.VARCHAR } )

        // We will validate the first rows content.
        .rows(new String[] {
          "1|Small Independent"
        } )

        // We won't validate all rows.
        .partial()
        .build();

    // Validate
    SqlContext.defaultContext().verify( expct );
  }

  /**
   * This test verifies that we can join two tables through the FROM clause.
   */
  @Test
  public void testOneWayJoin92() throws Exception {
    final SqlExpectation expct =
      newBuilder()

        // We join two tables in the FROM clause and select a column in each
        .query( "select warehouse.warehouse_id, warehouse_class.description from warehouse join warehouse_class on (warehouse.warehouse_class_id = warehouse_class.warehouse_class_id)" )

        // We expect two columns.
        .columns( new String[] { "warehouse_id", "description" } )

        // We want an integer and a string
        .types( new int[] { Types.INTEGER, Types.VARCHAR } )

        // We will validate the first rows content.
        .rows(new String[] {
          "1|Small Independent"
        } )

        // We won't validate all rows.
        .partial()
        .build();

    // Validate
    SqlContext.defaultContext().verify( expct );
  }
}
