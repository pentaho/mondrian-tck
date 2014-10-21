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
