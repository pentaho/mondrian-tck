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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import mondrian.spi.Dialect;

import org.junit.BeforeClass;
import org.junit.Test;

public class NullValuesTest {

  /**
   * Result null value as string
   */
  private static final String NULL_VALUE = "null";

  /**
   * Mondrian schema for check queries with null values
   */
  private static final String SCHEMA =
      "<Schema name=\"FoodMart\">"
      + "  <Cube name=\"Account\">"
      + "    <Table name=\"account\"></Table>"
      + "    <Dimension name=\"account\">\n"
      + "      <Hierarchy hasAll=\"true\" primaryKey=\"account_id\">\n"
      + "        <Table name=\"account\"/>\n"
      + "        <Level name=\"account parent\" type=\"Integer\" column=\"account_parent\" uniqueMembers=\"false\"/>\n"
      + "      </Hierarchy>\n"
      + "    </Dimension>"
      + "    <Dimension name=\"account custom member\">\n"
      + "      <Hierarchy hasAll=\"true\" primaryKey=\"account_id\">\n"
      + "        <Table name=\"account\"/>\n"
      + "        <Level name=\"custom member\" column=\"Custom_Members\" uniqueMembers=\"false\"/>\n"
      + "      </Hierarchy>\n"
      + "    </Dimension>"
      + "  </Cube>"
      + "</Schema>";

  private static final String ACCOUNT_TABLE_NAME = "account";

  private static final String ORDERED_COLUMN_NAME = "account_parent";

  /**
   * Result array parent's id for query with ordering
   */
  private String[] accountParentIds = {NULL_VALUE, NULL_VALUE, NULL_VALUE, "3000", "3000", "4000", "4000", "4000", "4000", "5000", "5000"};

  private static Dialect dialect;

  @BeforeClass
  public static void setUp() throws Exception {
    dialect = SqlContext.defaultContext().getDialect();
  }

  /**
   * Verifies MDX for select account parent by null key
   */
  @Test
  public void testSelectMembersWithNullKey() throws Exception {
    String orderExpr = getOrderExpression( "alias", "account.account_parent", true, true, true );
    MondrianExpectation expectation =
        MondrianExpectation.newBuilder()
            .query( "SELECT [account].[account].[account parent].[#null] on 0 FROM Account" )
            .result(
                "Axis #0:\n"
                    + "{}\n"
                    + "Axis #1:\n"
                    + "{[account].[#null]}\n"
                    + "Row #0: 3\n" )
            .sql(
                "select\n"
                    + "    " + dialect.quoteIdentifier( "account.account_parent" ) +" as " + dialect.quoteIdentifier( "c0" ) +"\n"
                    + "from\n"
                    + "    " + dialect.quoteIdentifier( "account" ) + " " + dialect.quoteIdentifier( "account" ) + "\n"
                    + "group by\n"
                    + "    " + dialect.quoteIdentifier( "account.account_parent" ) + "\n"
                    + "order by\n"
                    + "    " + orderExpr)
            .build();
    MondrianContext.forCatalog( SCHEMA ).verify( expectation );
  }

  /**
   * Verifies MDX for select custom members with order by null values
   */
  @Test
  public void testOrderMembersByNullValues() throws Exception {
    String orderExpr = getOrderExpression( "alias", "account.Custom_Members", true, true, true );
    MondrianExpectation expectation =
        MondrianExpectation
            .newBuilder()
            .query(
                "SELECT ORDER([account custom member].[account custom member].[custom member].members, [account custom member].[account custom member].[custom member].[#null], DESC) on 0 FROM Account" )
            .result(
                "Axis #0:\n"
                    + "{}\n"
                    + "Axis #1:\n"
                    + "{[account custom member].[#null]}\n"
                    + "{[account custom member].[LookUpCube(\"[Sales]]\",\"(Measures.[Store Sales]],\"+time.currentmember.UniqueName+\",\"+ Store.currentmember.UniqueName+\")\")]}\n"
                    + "Row #0: 10\n" + "Row #0: 1\n" )
            .sql(
                "select\n"
                    + "    " + dialect.quoteIdentifier( "account.Custom_Members" ) + " " + dialect.quoteIdentifier( "c0" ) + "\n"
                    + "from\n"
                    + "    " + dialect.quoteIdentifier( "account" ) + " " + dialect.quoteIdentifier( "account" ) + "\n"
                    + "where\n"
                    + "    UPPER(" + dialect.quoteIdentifier( "account.Custom_Members" ) + ") = UPPER('account custom member')\n"
                    + "group by\n"
                    + "    " + dialect.quoteIdentifier( "account.Custom_Members" ) + "\n"
                    + "order by\n"
                    + "    " + orderExpr)
            .build();
    MondrianContext.forCatalog( SCHEMA ).verify( expectation );
  }

  /**
   * Verifies order query by ascending with null values at first
   */
  @Test
  public void testOrderMembersAscWithNullFirst() throws Exception {
    Arrays.sort( accountParentIds, new StringWithNullComparator() );
    testOrderQuery( true, false );
  }

  /**
   * Verifies order query by ascending with null values at last
   */
  @Test
  public void testOrderMembersAscWithNullLast() throws Exception {
    Arrays.sort( accountParentIds, new StringWithNullComparator( false ) );
    testOrderQuery( true, true );
  }

  /**
   * Verifies order query by descending with null values at first
   */
  @Test
  public void testOrderMembersDescWithNullFirst() throws Exception {
    Arrays.sort( accountParentIds, Collections.reverseOrder( new StringWithNullComparator() ) );
    testOrderQuery( false, false );
  }

  /**
   * Verifies order query by descending with null values at last
   */
  @Test
  public void testOrderMembersDescWithNullLast() throws Exception {
    Arrays.sort( accountParentIds, Collections.reverseOrder( new StringWithNullComparator( false ) ) );
    testOrderQuery( false, true );
  }

  /**
   * A comparison function for ordering on some collection of objects string with null values. The function has custom
   * comparator for null and "null" values
   */
  private class StringWithNullComparator implements Comparator<String> {

    /**
     * If this is true than null values are moved to the head of the collection. Otherwise values are moved to the
     * bottom of the collection
     */
    private boolean isNullFirst = true;

    public StringWithNullComparator() {
    }

    public StringWithNullComparator( boolean isNullFirst ) {
      this.isNullFirst = isNullFirst;
    }

    @Override
    public int compare( String o1, String o2 ) {
      if ( o1 == null || o2 == null || o1.equals( NULL_VALUE ) || o2.equals( NULL_VALUE ) ) {
        return isNullFirst ? 1 : -1;
      }
      return o1.compareTo( o2 );
    }

  }

  private void testOrderQuery( boolean ascending, boolean collateNullsLast ) throws Exception {
    String orderExpr = getOrderExpression( "alias", ORDERED_COLUMN_NAME, true, ascending, collateNullsLast );
    final SqlExpectation expectation = newBuilder()
        .query( generateSelectQuery( ORDERED_COLUMN_NAME, ACCOUNT_TABLE_NAME, orderExpr ) )
        .columns( ORDERED_COLUMN_NAME )
        .rows( accountParentIds )
        .build();
    SqlContext.defaultContext().verify( expectation );
  }

  private String generateSelectQuery( String selectParam, String fromParam, String orderExpr ) {
    StringBuilder query = new StringBuilder( "select " );
    query.append( dialect.quoteIdentifier( selectParam ) );
    query.append( " from " );
    query.append( dialect.quoteIdentifier( fromParam ) );
    query.append( " order by " );
    query.append( orderExpr );
    return query.toString();
  }

  private String getOrderExpression( String alias, String expr, boolean nullable, boolean ascending,
      boolean collateNullsLast ) {
    String orderExpr =
        dialect.generateOrderItem( dialect.requiresOrderByAlias() ? dialect.quoteIdentifier( alias ) : dialect
            .quoteIdentifier( expr ), nullable, ascending, collateNullsLast );
    return orderExpr;
  }
}
