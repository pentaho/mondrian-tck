package org.pentaho.mondrian.tck;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

abstract class Context {

  public static final Properties testProperties;

  static {
    try {
      testProperties = loadTestProperties();
      if ( Boolean.parseBoolean( testProperties.getProperty( "register.big-data-plugin" ) ) ) {
        BigDataPluginUtil.prepareBigDataPlugin(
          new File( testProperties.getProperty( "big-data-plugin.folder" ) ),
          testProperties.getProperty( "active.hadoop.configuration" ) );
      }
    } catch ( Exception e ) {
      throw new RuntimeException( e );
    }
  }

  private static Properties loadTestProperties() throws IOException {
    Properties testProperties = new Properties();
    testProperties.load( new BufferedReader( new FileReader( "test.properties" ) ) );
    return testProperties;
  }
}
