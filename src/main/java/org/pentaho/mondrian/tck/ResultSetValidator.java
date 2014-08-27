package org.pentaho.mondrian.tck;


import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ResultSetValidator {
    final String[] columns;
    final boolean columnsPartial;
    final String[] rows;
    final boolean partial;
    final int[] types;

    public ResultSetValidator( String[] columns, boolean columnsPartial, String[] rows, boolean partial, int[] types ) {
        this.columns = columns;
        this.columnsPartial = columnsPartial;
        this.rows = rows;
        this.partial = partial;
        this.types = types;
    }

    public void validateColumns( ResultSet rs ) throws Exception {
        if ( columns == null ) {
            return;
        }
        // some drivers return lower case, some - upper.
        final String[] actuals = rsToColumns( rs, true );
        if ( columnsPartial ) {
            for ( String column : columns ) {
                assertTrue( String.format( "Column '%s' doesn't exist in the columns result set '%s'", column,
                        Arrays.toString(actuals) ), Arrays.asList( actuals ).contains( column.toLowerCase() ) );
            }
        } else {
            assertArrayEquals(
                    "Column names do not correspond to those expected.",
                    columns,
                    actuals );
        }
    }

    public void validateRows( ResultSet rs ) throws Exception {
    final int nbCols = columnsPartial ? columns.length : rs.getMetaData().getColumnCount();
    final List<String> columnsList = columnsPartial ? Arrays.asList( columns ) : new ArrayList<String>();

        int rowNum = -1;
        while ( rs.next() ) {

            final StringBuilder curRow = new StringBuilder();

            // Check whether the RS has more rows but we were not expecting more
            if ( rows != null && ++rowNum >= rows.length && !partial ) {
                fail( "ResultSet returned more rows than expected" );
            }

            // Build a string representation of the row
            for ( int j = 1; j <= nbCols; j++ ) {
              if ( columnsPartial && !columnsList.contains( rs.getMetaData().getColumnName( j ).toLowerCase() ) ) {
                continue;
              }

                if ( j > 1 ) {
                    // Add a delimiter
                    curRow.append( "|" );
                }

                final Object rawValue = rs.getObject( j );

                // Validate types
                if ( types != null ) {
                    validateMetaType( rs );
                    validateType( rs.getMetaData().getColumnName( j ), rawValue, types[j - 1] );
                }

                // Print the value to the buffer.
        switch ( rs.getMetaData().getColumnType( j ) ) {
          case Types.DOUBLE:
          case Types.DECIMAL:
            curRow.append( new DecimalFormat().format( rawValue ) );
            break;
          default:
                curRow.append( String.valueOf( rawValue ) );
            }
      }

            // Now validate that row
            if ( rows != null ) {
                assertEquals(
                        "Row content doesn't match.",
                        rows[rowNum],
                        curRow.toString() );
            } else {
                // There was no row defined. we bail now.
                break;
            }

            // If there are still rows and we're not in partial mode, that's bad
            if ( !partial && rowNum > ( rows.length - 1 ) ) {
                fail( "Expected number of rows doesn't match the result." );
            }

            // Check whether we have reached the limit of the rows to validate
            if ( partial && rowNum == ( rows.length - 1 ) ) {
                break;
            }
        }
    }

    private void validateMetaType( ResultSet rs ) throws Exception {
        if ( types == null ) {
            return;
        }
    int columnCount = columnsPartial ? columns.length : rs.getMetaData().getColumnCount();
    final List<String> columnsList = columnsPartial ? Arrays.asList( columns ) : new ArrayList<String>();
        for ( int i = 1; i <= columnCount; i++ ) {
      if ( columnsPartial && !columnsList.contains( rs.getMetaData().getColumnName( i ).toLowerCase() ) ) {
        continue;
      }
            assertEquals(
                    "Wrong meta type for column " + rs.getMetaData().getColumnName( i )
                            + ", expected meta type "
                            + types[i - 1]
                            + " but actual meta type was " + rs.getMetaData().getColumnType( i ),
                    types[i - 1],
                    rs.getMetaData().getColumnType( i ) );
        }
    }

    private void validateType( String colName, Object actual, int expected ) throws Exception {
        // skip null value
        if ( actual == null ) {
            return;
        }
        switch ( expected ) {
            case java.sql.Types.BIGINT:
                checkType( colName, "Long", actual.getClass(), Long.class );
                break;

            case java.sql.Types.DECIMAL:
                checkType( colName, "Double / BigDecimal", actual.getClass(), Double.class, BigDecimal.class );
                break;

            case java.sql.Types.BOOLEAN:
                checkType( colName, "Boolean", actual.getClass(), Boolean.class );
                break;

            case java.sql.Types.INTEGER:
            case java.sql.Types.SMALLINT:
                checkType( colName, "Integer", actual.getClass(), Integer.class );
                break;

            case java.sql.Types.VARCHAR:
            case java.sql.Types.CHAR:
                checkType( colName, "String", actual.getClass(), String.class );
                break;

            case java.sql.Types.DOUBLE:
                checkType( colName, "Double", actual.getClass(), Double.class );
                break;

            case java.sql.Types.TINYINT:
                checkType( colName, "Byte", actual.getClass(), Byte.class );
                break;

            default:
                throw new Exception( "Expected type check not implemented." );
        }
    }

  private void checkType( String colName, String expectedType, Class<?> actualTypeClass, Class<?>... expectedTypeClass ) {
        assertTrue(
                "Wrong type for column " + colName
                        + ", expected type " + expectedType + " but object class was " + actualTypeClass.getName(),
                Arrays.asList( expectedTypeClass ).contains( actualTypeClass ) );
    }

    private String[] rsToColumns( ResultSet rs, boolean lowerCase ) throws Exception {
        final List<String> effectiveCols = new ArrayList<>();
        final ResultSetMetaData rsm = rs.getMetaData();
        for ( int i = 1; i <= rsm.getColumnCount(); i++ ) {
            final String columnName = rsm.getColumnName( i );
            effectiveCols.add( lowerCase ? columnName.toLowerCase() : columnName );
        }
        return effectiveCols.toArray( new String[ effectiveCols.size() ] );
    }

}
