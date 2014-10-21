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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import mondrian.rolap.RolapUtil;

import org.apache.log4j.Logger;

import com.google.common.base.Function;

public class SqlExpectation {

  static final Logger LOGGER = RolapUtil.SQL_LOGGER;
  final ResultSetProvider query;
  final String[] columns;
  final boolean columnsPartial;
  final String[] rows;
  final boolean partial;
  final int[] types;
  List<Function<Statement, Void>> statementModifiers;
  final int cancelTimeout;
  final ResultSetValidator validator;

  public SqlExpectation(
      ResultSetProvider query,
      String[] columns,
      boolean columnsPartial,
      int[] types,
      String[] rows,
      boolean partial,
      int cancelTimeout,
      final List<Function<Statement, Void>> statementModifiers ) {

    this.query = query;
    this.columns = columns;
    this.columnsPartial = columnsPartial;
    this.types = types;
    this.rows = rows;
    this.partial = partial;
    this.cancelTimeout = cancelTimeout;
    this.statementModifiers = statementModifiers;
    this.validator = new ResultSetValidator( columns, columnsPartial, rows, partial, types );
  }

  public void verify( ResultSet rs ) throws Exception {
    // Validate column names
    validator.validateColumns( rs );
    // Validate rows
    validator.validateRows( rs );
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
    private int cancelTimeout = -1;
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
        public ResultSet getData( Connection conn, final Statement statement ) throws Exception {
          for ( Function<Statement, Void> statementModifier : statementModifiers ) {
            statementModifier.apply( statement );
          }

          try {
            // Run the query
            SqlExpectation.LOGGER.info( "Mondrian.tck:" + query );
            statement.execute( query );
          } catch ( Throwable t ) {
            throw new Exception(
              "Query failed to run successfully:\n"
              + query,
              t );
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

    public Builder cancelTimeout( int to ) {
      this.cancelTimeout = to;
      return this;
    }

    public SqlExpectation build() {
      return new SqlExpectation( query, columns, columnsPartial, types, rows, partial, cancelTimeout, statementModifiers );
    }
  }

  /**
   * This interface has to be implemented to provide a ResultSet to validate to
   * the Expectation classes.
   *
   * <p>There are two arguments to the API, one for the connection and one for the
   * statement. Note that this is required because the statements provided by the shims
   * are not symmetrical. The bug can be represented as:
   *
   * <p><code>connection != connection.createStatement().getConnection()</code>
   */
  public interface ResultSetProvider {
    /**
     * Returns {@link java.sql.ResultSet} executed by the {@link java.sql.Statement}<br/>
     * <p>
     *   Code should be like <br/>
     *   {@code statement.<doSomething>; return statement.getResultSet();}
     * </p>
     */
    ResultSet getData( Connection conn, Statement statement ) throws Exception;
  }
}
