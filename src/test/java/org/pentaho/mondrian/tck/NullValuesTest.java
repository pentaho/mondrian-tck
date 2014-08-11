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

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

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
      + "        <Level name=\"custom member\" column=\"custom_members\" uniqueMembers=\"false\"/>\n"
      + "      </Hierarchy>\n"
      + "    </Dimension>"
      + "  </Cube>"
      + "</Schema>";

  /**
   * Result array parent's id for query with ordering
   */
  private String[] accountParentIds = {NULL_VALUE, NULL_VALUE, NULL_VALUE, "3000", "3000", "4000", "4000", "4000", "4000", "5000", "5000"};

  /**
   * Verifies MDX for select account parent by null key
   */
  @Test
  public void testSelectMembersWithNullKey() throws Exception {
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
                    + "    account.account_parent as c0\n"
                    + "from\n"
                    + "    account account\n"
                    + "group by\n"
                    + "    account.account_parent\n"
                    + "order by\n"
                    + "    CASE WHEN account.account_parent IS NULL THEN 1 ELSE 0 END, account.account_parent ASC" )
            .build();
    MondrianContext.forCatalog( SCHEMA ).verify( expectation );
  }

  /**
   * Verifies MDX for select custom members with order by null values
   */
  @Test
  public void testOrderMembersByNullValues() throws Exception {
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
                    + "    account.custom_members c0\n"
                    + "from\n"
                    + "    account account\n"
                    + "where\n"
                    + "    UPPER(account.custom_members) = UPPER('account custom member')\n"
                    + "group by\n"
                    + "    account.custom_members\n"
                    + "order by\n"
                    + "    CASE WHEN account.custom_members IS NULL THEN 1 ELSE 0 END, account.custom_members ASC" )
            .build();
    MondrianContext.forCatalog( SCHEMA ).verify( expectation );
  }

  /**
   * Verifies order query by ascending with null values at first
   */
  @Test
  public void testOrderMembersAscWithNullFirst() throws Exception {
    Arrays.sort( accountParentIds, new StringWithNullComparator() );
    final SqlExpectation expectation = newBuilder()
        .query( "select account.account_parent account_parent from account order by CASE WHEN account.account_parent IS NULL THEN 0 ELSE 1 END, account.account_parent ASC" )
        .columns( "account_parent" )
        .types( Types.INTEGER )
        .rows( accountParentIds )
        .build();

    SqlContext.defaultContext().verify( expectation );
  }

  /**
   * Verifies order query by ascending with null values at last
   */
  @Test
  public void testOrderMembersAscWithNullLast() throws Exception {
    Arrays.sort( accountParentIds, new StringWithNullComparator( false ) );
    final SqlExpectation expectation = newBuilder()
        .query( "select account.account_parent account_parent from account order by CASE WHEN account.account_parent IS NULL THEN 1 ELSE 0 END, account.account_parent ASC" )
        .columns( "account_parent" )
        .types( Types.INTEGER )
        .rows( accountParentIds )
        .build();

    SqlContext.defaultContext().verify( expectation );
  }

  /**
   * Verifies order query by descending with null values at first
   */
  @Test
  public void testOrderMembersDescWithNullFirst() throws Exception {
    Arrays.sort( accountParentIds, Collections.reverseOrder( new StringWithNullComparator() ) );
    final SqlExpectation expectation = newBuilder()
        .query( "select account.account_parent account_parent from account order by CASE WHEN account.account_parent IS NULL THEN 0 ELSE 1 END, account.account_parent DESC" )
        .columns( "account_parent" )
        .types( Types.INTEGER )
        .rows( accountParentIds )
        .build();

    SqlContext.defaultContext().verify( expectation );
  }

  /**
   * Verifies order query by descending with null values at last
   */
  @Test
  public void testOrderMembersDescWithNullLast() throws Exception {
    Arrays.sort( accountParentIds, Collections.reverseOrder( new StringWithNullComparator( false ) ) );
    final SqlExpectation expectation = newBuilder()
        .query( "select account.account_parent account_parent from account order by CASE WHEN account.account_parent IS NULL THEN 1 ELSE 0 END, account.account_parent DESC" )
        .columns( "account_parent" )
        .types( Types.INTEGER )
        .rows( accountParentIds )
        .build();

    SqlContext.defaultContext().verify( expectation );
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
}
