package org.pentaho.mondrian.tck;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

public class SqlExpectation {

  final String query;
  final String[] columns;
  final String[] rows;
  final boolean partial;
  final int[] types;

  public SqlExpectation( String query, String[] columns, int[] types, String[] rows, boolean partial ) {
    this.query = query;
    this.columns = columns;
    this.types = types;
    this.rows = rows;
    this.partial = partial;
  }

  public void verify( ResultSet rs ) throws Exception {
    // Validate column names
    assertEquals(
      "Column names do not correspond to those expected.",
      columns,
      rsToColumns( rs ) );
    // Validate rows
    validateRows( rs );
  }

  private String[] rsToColumns( ResultSet rs ) throws Exception {
    final List<String> effectiveCols = new ArrayList<String>();
    final ResultSetMetaData rsm = rs.getMetaData();
    for ( int i = 1; i <= rsm.getColumnCount(); i++ ) {
      effectiveCols.add( rsm.getColumnName( i ) );
    }
    return effectiveCols.toArray( new String[ effectiveCols.size() ] );
  }

  private void validateRows( ResultSet rs ) throws Exception {
    final int nbCols = rs.getMetaData().getColumnCount();
    if ( rows != null ) {
      for ( int i = 1; i <= rows.length; i++ ) {
        final StringBuilder curRow = new StringBuilder();
        rs.next();
        for ( int j = 1; j <= nbCols; j++ ) {
          Object rawValue = rs.getObject( j );
          validateType( rawValue, types[j] );
          curRow.append( rawValue.toString() );
        }
      }
    }
  }

  private void validateType( Object actual, int expected ) throws Exception {
    switch ( expected ) {
      case java.sql.Types.BIGINT:
        assertTrue(
          "Wrong type for column.",
          actual.getClass().equals( Long.class ) );
        break;

      case java.sql.Types.BOOLEAN:
        assertTrue(
          "Wrong type for column.",
          actual.getClass().equals( Boolean.class ) );
        break;

      case java.sql.Types.INTEGER:
        assertTrue(
          "Wrong type for column.",
          actual.getClass().equals( Integer.class ) );
        break;

      case java.sql.Types.VARCHAR:
        assertTrue(
          "Wrong type for column.",
          actual.getClass().equals( String.class ) );
        break;

      default:
        throw new Exception( "Expected type check not implemented." );
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String query;
    private String[] columns;
    private String[] rows;
    private int[] types;
    private boolean partial = false;

    private Builder() {
    }

    public Builder query( String query ) {
      this.query = query;
      return this;
    }

    public Builder columns( String[] columns ) {
      this.columns = columns;
      return this;
    }

    public Builder types( int[] types ) {
      this.types = types;
      return this;
    }

    public Builder rows( String[] rows ) {
      this.rows = rows;
      return this;
    }

    public Builder partial() {
      this.partial  = true;
      return this;
    }

    public SqlExpectation build() {
      return new SqlExpectation( query, columns, types, rows, partial );
    }
  }
}
