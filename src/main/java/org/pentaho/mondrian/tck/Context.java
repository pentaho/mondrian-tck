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
