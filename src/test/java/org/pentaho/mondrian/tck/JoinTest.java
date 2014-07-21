package org.pentaho.mondrian.tck;

import static org.pentaho.mondrian.tck.SqlExpectation.newBuilder;

import java.sql.Types;

import org.junit.Test;

public class JoinTest {

  /**
   * This test verifies that we can join two tables
   */
  @Test
  public void testOneWayJoin89() throws Exception {
    SqlExpectation expct =
      newBuilder()
        .query( "select warehouse.warehouse_id, warehouse_class.description from warehouse, warehouse_class where warehouse.warehouse_class_id = warehouse_class.warehouse_class_id" )
        .columns( new String[] { "warehouse_id", "description" } )
        .types( new int[] { Types.INTEGER, Types.VARCHAR } )
        .build();
    // Validate
    SqlContext.defaultContext().verify( expct );
  }
}
