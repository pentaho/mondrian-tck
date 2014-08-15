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

import com.google.common.base.Function;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SqlExpectation {

  final ResultSetProvider query;
  final String[] columns;
  final boolean columnsPartial;
  final String[] rows;
  final boolean partial;
  final int[] types;
  List<Function<Statement, Void>> statementModifiers;

  final ResultSetValidator validator;

  public SqlExpectation( ResultSetProvider query, String[] columns, boolean columnsPartial, int[] types, String[] rows,
                         boolean partial, final List<Function<Statement, Void>> statementModifiers ) {
    this.query = query;
    this.columns = columns;
    this.columnsPartial = columnsPartial;
    this.types = types;
    this.rows = rows;
    this.partial = partial;
    this.statementModifiers = statementModifiers;
    this.validator = new ResultSetValidator(columns, columnsPartial, rows, partial, types);

  }

  public void verify( ResultSet rs ) throws Exception {
    // Validate column names
    validator.validateColumns(rs);
    // Validate rows
    validator.validateRows(rs);
  }


  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private ResultSetProvider query;
    private String[] columns;
    private boolean columnsPartial;
    private String[] rows;
    private int[] types;
    private boolean partial = false;
    private List<Function<Statement, Void>> statementModifiers = new ArrayList<>();

    private Builder() {
    }

    /**
     * Sets the {@link ResultSetProvider} to run.
     * <p>(mandatory)
     */
    public Builder query( ResultSetProvider query ) {
      this.query = query;
      return this;
    }

    /**
     * Sets the SQL query to run.
     * <p>(mandatory)
     */
    public Builder query( final String query ) {
      return query( new ResultSetProvider() {
        @Override
        public ResultSet getData( Statement statement ) throws Exception {
          for ( Function<Statement, Void> statementModifier : statementModifiers ) {
            statementModifier.apply( statement );
          }

          try {
            statement.execute( query );
          } catch ( Throwable t ) {
            throw new Exception( "Query failed to run successfully.", t );
          }

          return statement.getResultSet();
        }
      } );
    }

    /**
     * Sets the column names expected
     * <p>(optional)
     */
    public Builder columns( String... columns ) {
      this.columns = columns;
      return this;
    }

    /**
     * Sets whether the columns provided in {@link #columns(String[])} are only the
     * part of the columns of the result set.
     * <p>(optional)
     */
    public Builder columnsPartial() {
      this.columnsPartial = true;
      return this;
    }

    /**
     * Sets the expected column types. Use values in {@link java.sql.Types}.
     * <p>(optional)
     */
    public Builder types( int... types ) {
      this.types = types;
      return this;
    }

    /**
     * Sets the expected rows. The value delimiter is pipe ( '|' ).
     * <p>(optional)
     */
    public Builder rows( String... rows ) {
      this.rows = rows;
      return this;
    }

    /**
     * Sets whether the rows provided in {@link #rows(String[])} are only the
     * first rows of the result set and we didn't intend to validate them all.
     */
    public Builder partial() {
      this.partial  = true;
      return this;
    }

    /**
     * adds a function that will be run for the statement before execution
     */
    public Builder modifyStatement( Function<Statement, Void> statementModifier ) {
      statementModifiers.add( statementModifier );
      return this;
    }

    public SqlExpectation build() {
      return new SqlExpectation( query, columns, columnsPartial, types, rows, partial, statementModifiers );
    }
  }

  public interface ResultSetProvider {
    ResultSet getData( Statement statement ) throws Exception;
  }
}
