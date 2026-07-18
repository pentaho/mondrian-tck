/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2013 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/

package org.pentaho.mondrian.tck;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.Properties;

import static java.nio.file.Files.newBufferedReader;

abstract class Context {

  public static final Properties testProperties;

  static {
    try {
      testProperties = loadTestProperties();
      if ( Boolean.parseBoolean( testProperties.getProperty( "register.big-data-plugin" ) ) ) {
        BigDataPluginUtil.prepareBigDataPlugin(
            Paths.get( testProperties.getProperty( "big-data-plugin.folder" ) ),
            testProperties.getProperty( "active.hadoop.configuration" ) );
      }
    } catch ( Exception e ) {
      throw new RuntimeException( e );
    }
  }

  private static Properties loadTestProperties() throws IOException {
    Properties testProperties = new Properties();
    try ( BufferedReader reader = newBufferedReader( Paths.get( "test.properties" ), Charset.defaultCharset() ) ) {
      testProperties.load( reader );
    }
    return testProperties;
  }
}
