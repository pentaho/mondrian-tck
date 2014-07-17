package org.pentaho.mondrian.tck;

import com.google.common.base.Optional;
import org.olap4j.CellSet;
import org.olap4j.layout.TraditionalCellSetFormatter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MondrianExpectation {
  private final String query;
  private final List<String> expectedSqls;
  private final Optional<String> result;

  private MondrianExpectation(
    final String query, final String result, final List<String> expectedSqls ) {
    this.query = query;
    this.expectedSqls = expectedSqls;
    this.result = Optional.fromNullable( result );
  }

  public String getQuery() {
    return query;
  }

  public void verify( CellSet cellSet, List<String> sqls ) {
    if ( result.isPresent() ) {
      assertEquals( result.get(), cellSetToString( cellSet ) );
    }
    for ( String expectedSql : this.expectedSqls ) {
      assertTrue( "Expected sql was not executed: \n" + expectedSql, sqls.contains( expectedSql ) );
    }
  }

  private String cellSetToString( CellSet cellSet ) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      final PrintWriter pw = new PrintWriter( baos );
      new TraditionalCellSetFormatter().format( cellSet, pw );
      pw.flush();
    } finally {
      try {
        baos.close();
      } catch ( IOException e ) {
        e.printStackTrace();
      }
    }
    return baos.toString().replaceAll( "\r\n", "\n" );
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String result;
    private List<String> sqls = new ArrayList<>();
    private String query;

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

    public MondrianExpectation build() {
      return new MondrianExpectation( query, result, sqls );
    }

  }
}
