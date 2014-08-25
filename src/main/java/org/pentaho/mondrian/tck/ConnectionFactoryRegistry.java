package org.pentaho.mondrian.tck;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;

import mondrian.olap.MondrianProperties;
import mondrian.rolap.RolapUtil;

public class ConnectionFactoryRegistry extends Context {

  static {
    RolapUtil.loadDrivers( MondrianProperties.instance().JdbcDrivers.get() );
  }

  public static TestConnectionFactory getDriverManagerConnectionFactory(String url) {
    String user = MondrianProperties.instance().TestJdbcUser.get();
    String password = MondrianProperties.instance().TestJdbcPassword.get();
    return new DriverManagerConnectionFactory( url, user, password );
  }

  public static TestConnectionFactory getDBCPConnectionFactory(String url , int poolSize ) {
    String user = MondrianProperties.instance().TestJdbcUser.get();
    String password = MondrianProperties.instance().TestJdbcPassword.get();
    return new DBCPConnectionFactory( url, user, password, poolSize );
  }

  private static final class DBCPConnectionFactory implements TestConnectionFactory {

    private final BasicDataSource dataSource;

    public DBCPConnectionFactory( final String connectURI, final String user, final String password, int poolSize ) {
      BasicDataSource basicDataSource = new BasicDataSource();
      basicDataSource.setDriverClassName( MondrianProperties.instance().JdbcDrivers.get() );
      basicDataSource.setUsername( user );
      basicDataSource.setPassword( password );
      basicDataSource.setUrl( connectURI );
      basicDataSource.setAccessToUnderlyingConnectionAllowed( true );
      basicDataSource.setMaxActive( poolSize );
      this.dataSource = basicDataSource;
    }

    @Override
    public Connection getConnection() throws SQLException {
      return dataSource.getConnection();
    }
  }

  private static final class DriverManagerConnectionFactory implements TestConnectionFactory {

    private final String dbConnectURI;

    private final String defaultUser;

    private final String defaultPassword;

    public DriverManagerConnectionFactory( final String connectURI, final String user, final String password ) {
      dbConnectURI = connectURI;
      defaultUser = user;
      defaultPassword = password;
    }

    @Override
    public Connection getConnection() throws SQLException {
      return DriverManager.getConnection( dbConnectURI, defaultUser, defaultPassword );
    }
  }
}
