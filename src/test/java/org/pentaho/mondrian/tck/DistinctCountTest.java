package org.pentaho.mondrian.tck;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import mondrian.olap.MondrianProperties;

import org.junit.Test;

import com.google.common.base.Function;

public class DistinctCountTest {

  @Test
  public void testSingleColumnSQL() throws Exception {
    SqlContext sqlContext = SqlContext.defaultContext();
    SqlExpectation sqlExpectation = SqlExpectation.newBuilder()
      .query( "select count(distinct(customer_id)) from sales_fact_1997" )
      .rows( "5581" )
      .build();
    sqlContext.verify( sqlExpectation );
  }
  
  @Test
  public void testMultipleColumnSQL() throws Exception {
    SqlContext sqlContext = SqlContext.defaultContext();
    SqlExpectation sqlExpectation = SqlExpectation.newBuilder()
      .query( "select count(distinct(customer_id)), count(distinct(product_id))  from sales_fact_1997" )
      .rows( "5581", "1559" )
      .build();
    sqlContext.verify( sqlExpectation );
  }
  
  @Test
  public void testCompoundColumnSQL() throws Exception {
    SqlContext sqlContext = SqlContext.defaultContext();
    SqlExpectation sqlExpectation = SqlExpectation.newBuilder()
      .query( "select count( distinct customer_id, product_id) from sales_fact_1997" )
      .rows( "85452" )
      .build();
    sqlContext.verify( sqlExpectation );
  }
  
  @Test
  public void testJDBCIndexes() throws Exception {
    
    SqlContext sqlContext = SqlContext.defaultContext();
    sqlContext.executeJDBCCode( new SqlContext.IJDBCExecutable() {
      public void executeCode( Connection conn ) throws Exception {
        // real code goes here once I find driver supporting Index info
        ResultSet rs = conn.getMetaData().getIndexInfo( null, null, "sales_fact_1997", false, false );
        
        rs.first();
        while(rs.isAfterLast()){
          for(int i=0; i<rs.getMetaData().getColumnCount(); i++){
            System.out.println(rs.getMetaData().getColumnName( i ));
          }
          System.out.println();
          rs.next();
        }
      }
    });
  }
}
