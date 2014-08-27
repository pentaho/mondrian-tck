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

import com.google.common.base.Optional;

import mondrian.spi.Dialect;

import org.olap4j.CellSet;
import org.olap4j.layout.TraditionalCellSetFormatter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MondrianExpectation {
  private final String query;
  private final List<String> expectedSqls;
  private final Optional<String> result;
  private final boolean expectResultSet;
  private ResultSetValidator rsValidator;
  final boolean canBeRandomlyCanceled;
  final boolean withFreshCache;

  public MondrianExpectation(
      final String query,
      final List<String> expectedSqls,
      final String result,
      final boolean expectResultSet,
      final String[] columns,
      final boolean columnsPartial,
      final String[] rows,
      final boolean partial,
      final int[] types,
      final boolean canBeRandomlyCanceled,
      final boolean withFreshCache ) {
    this.query = query;
    this.expectedSqls = expectedSqls;
    this.canBeRandomlyCanceled = canBeRandomlyCanceled;
    this.withFreshCache = withFreshCache;
    this.result = Optional.fromNullable( result );
    this.expectResultSet = expectResultSet;
    if ( this.expectResultSet ) {
        rsValidator = new ResultSetValidator( columns, columnsPartial, rows, partial, types );
    }
  }

  public String getQuery() {
    return query;
  }

  public boolean isExpectResultSet() {
    return expectResultSet;
  }

  public void verify( ResultSet rs, List<String> sqls, Dialect dialect ) throws Exception {
    rsValidator.validateRows( rs );
    rsValidator.validateColumns( rs );
    verifySqls( sqls, dialect );
  }

  public void verify( CellSet cellSet, List<String> sqls, Dialect dialect ) {
    if ( result.isPresent() ) {
      assertEquals( result.get(), cellSetToString( cellSet ) );
    }
    verifySqls( sqls, dialect );
  }

  protected void verifySqls( List<String> sqls, Dialect dialect ) {
    List<String> cleanSqls = new ArrayList<>();
    for ( String sql : sqls ) {
      sql = cleanLineEndings( sql );
      sql = cleanTicks( sql, dialect );
      sql = cleanAlias( sql, dialect );
      cleanSqls.add( sql );
    }

    for ( String expectedSql : this.expectedSqls ) {
      expectedSql = cleanLineEndings( expectedSql );
      expectedSql = cleanTicks( expectedSql, dialect );
      expectedSql = cleanAlias( expectedSql, dialect );
      boolean found = false;
      for ( String cleanString : cleanSqls ) {
        if ( cleanString.contains( expectedSql ) ) {
          found = true;
          break;
        }
      }
      assertTrue(
        "Expected sql was not executed: \n" + expectedSql,
        found );
    }
  }

  private String cellSetToString( CellSet cellSet ) {
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    try {
      final PrintWriter pw = new PrintWriter( stream );
      new TraditionalCellSetFormatter().format( cellSet, pw );
      pw.flush();
    } finally {
      try {
        stream.close();
      } catch ( IOException e ) {
        e.printStackTrace();
      }
    }
    return stream.toString().replaceAll( "\r\n", "\n" );
  }

  private static String cleanLineEndings( String string ) {
    return string.replaceAll( "\r\n", "\n" );
  }

  private static String cleanTicks( String sql, Dialect dialect ) {
    return sql.replaceAll( "\\" + dialect.getQuoteIdentifierString(), "" );
  }

  private static String cleanAlias( String sql, Dialect dialect ) {
    return sql.replaceAll( "\\sas\\s", " " );
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String result;
    private List<String> sqls = new ArrayList<>();
    private String query;
    private String[] columns;
    private boolean columnsPartial;
    private String[] rows;
    private int[] types;
    private boolean partial = false;
    private boolean expectResultSet = false;
    private boolean canBeRandomlyCanceled = false;
    private boolean withFreshCache = false;

    private Builder() {
    }

    public Builder query( String query ) {
      this.query = query;
      return this;
    }

    public Builder result( String result ) {
      this.result = result;
      return this;
    }

    public Builder sql( String sql ) {
      sqls.add( sql );
      return this;
    }

    public Builder expectResultSet( boolean expectResultSet ) {
      this.expectResultSet = expectResultSet;
      return this;
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

    public Builder canBeRandomlyCanceled() {
      this.canBeRandomlyCanceled = true;
      return this;
    }

    public Builder withFreshCache() {
      this.withFreshCache = true;
      return this;
    }

    public MondrianExpectation build() {
      return new MondrianExpectation( query, sqls, result, expectResultSet, columns, columnsPartial, rows, partial, types, canBeRandomlyCanceled, withFreshCache );
    }

  }
}
