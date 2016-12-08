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
 * may describe, in whole or in part. Any reproduction, modification,
 * distribution, or public display of this information without the express
 * written authorization from Pentaho is strictly prohibited and in violation
 * of applicable laws and international treaties. Access to the source code
 * contained herein is strictly prohibited to anyone except those individuals
 * and entities who have executed confidentiality and non-disclosure agreements
 * or other agreements with Pentaho, explicitly covering such access.
 */
package org.pentaho.mondrian.tck;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import mondrian.olap.MondrianProperties;
import mondrian.rolap.RolapUtil;

import org.apache.commons.dbcp.BasicDataSource;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * A registry for test connection factories.
 */
public final class ConnectionFactoryRegistry extends Context {

  /**
   * A cache for connection factories based on data sources.
   */
  private static final LoadingCache<String, TestConnectionFactory>
      DATA_SOURCE_CONNECTION_FACTORY_CACHE = CacheBuilder
      .newBuilder().build(new CacheLoader<String, TestConnectionFactory>() {

        @Override
        public TestConnectionFactory load(final String connectURI)
            throws Exception {
          return new DBCPConnectionFactory(connectURI,
              MondrianProperties.instance().TestJdbcUser.get(),
              MondrianProperties.instance().TestJdbcPassword.get());
        }
      });

  /**
   * A cache for connection factories based on {@link DriverManager}.
   */
  private static final LoadingCache<String, TestConnectionFactory>
      DRIVER_MANAGER_CONNECTION_FACTORY_CACHE = CacheBuilder
      .newBuilder().build(new CacheLoader<String, TestConnectionFactory>() {

        @Override
        public TestConnectionFactory load(final String connectURI)
            throws Exception {
          return new DriverManagerConnectionFactory(connectURI,
              MondrianProperties.instance().TestJdbcUser.get(),
              MondrianProperties.instance().TestJdbcPassword.get());
        }

      });

  static {
    RolapUtil.loadDrivers(MondrianProperties.instance().JdbcDrivers.get());
  }

  /**
   * A private constructor to prevent instantiation.
   */
  private ConnectionFactoryRegistry() {
  }

  /**
   * Creates {@link DriverManagerConnectionFactory} with default connection URI.
   *
   * @return test connection factory based on {@link DriverManager}
   * @throws ExecutionException if an error occurs
   */
  public static TestConnectionFactory
      getDefaultDriverManagerConnectionFactory() throws ExecutionException {
    return getDriverManagerConnectionFactory(MondrianProperties.instance()
        .FoodmartJdbcURL.get());
  }

  /**
   * Creates {@link DBCPConnectionFactory} with default connection URI.
   *
   * @return test connection factory based on DBCP connection pool
   * @throws ExecutionException if an error occurs
   */
  public static TestConnectionFactory getDefaultDBCPConnectionFactory()
      throws ExecutionException {
    return getDBCPConnectionFactory(MondrianProperties.instance()
        .FoodmartJdbcURL.get());
  }

  /**
   * Creates {@link DriverManagerConnectionFactory}.
   *
   * @param connectionURI database connection URI
   * @return test connection factory based on {@link DriverManager}
   * @throws ExecutionException if an error occurs
   */
  public static TestConnectionFactory
      getDriverManagerConnectionFactory(final String connectionURI)
      throws ExecutionException {
    return DRIVER_MANAGER_CONNECTION_FACTORY_CACHE.get(connectionURI);
  }

  /**
   * Creates {@link DBCPConnectionFactory}.
   *
   * @param connectionURI database connection URI
   * @return test connection factory based on DBCP connection pool
   * @throws ExecutionException if an error occurs
   */
  public static TestConnectionFactory
      getDBCPConnectionFactory(final String connectionURI)
          throws ExecutionException {
    return DATA_SOURCE_CONNECTION_FACTORY_CACHE.get(connectionURI);
  }

  /**
   * DBCP data source based implementation of {@link TestConnectionFactory}.
   */
  private static final class DBCPConnectionFactory
      implements TestConnectionFactory {

    /**
     * A source of connections.
     */
    private final BasicDataSource dataSource;

    /**
     * Creates a DriverManagerConnectionFactory.
     *
     * @param connectURI database URI
     * @param user user login
     * @param password user password
     */
    public DBCPConnectionFactory(final String connectURI, final String user,
        final String password) {
      BasicDataSource basicDataSource = new BasicDataSource();
      basicDataSource.setDriverClassName(MondrianProperties.instance()
          .JdbcDrivers.get());
      basicDataSource.setUsername(user);
      basicDataSource.setPassword(password);
      basicDataSource.setUrl(connectURI);
      basicDataSource.setAccessToUnderlyingConnectionAllowed(true);
      this.dataSource = basicDataSource;
    }

    @Override
    public Connection createConnection() throws SQLException {
      return dataSource.getConnection();
    }

  }

  /**
   * {@link DriverManagerConnectionFactory} based implementation of
   * {@link TestConnectionFactory}.
   */
  private static final class DriverManagerConnectionFactory
      implements TestConnectionFactory {

    /**
     * A database connection URI.
     */
    private final String dbConnectURI;

    /**
     * A default user name.
     */
    private final String defaultUser;

    /**
     * A password for default user.
     */
    private final String defaultPassword;

    /**
     * Creates a DriverManagerConnectionFactory.
     *
     * @param connectURI database URI
     * @param user user's login
     * @param password user's password
     */
    public DriverManagerConnectionFactory(final String connectURI,
        final String user, final String password) {
      dbConnectURI = connectURI;
      defaultUser = user;
      defaultPassword = password;
    }

    @Override
    public Connection createConnection() throws SQLException {
      return DriverManager.getConnection(dbConnectURI, defaultUser,
          defaultPassword);
    }

  }

}
