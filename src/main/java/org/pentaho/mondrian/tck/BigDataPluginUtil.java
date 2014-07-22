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

import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.gui.GUIOption;
import org.pentaho.di.core.lifecycle.LifecycleListener;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.Plugin;
import org.pentaho.di.core.plugins.PluginRegistry;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.nio.file.Files.*;

public class BigDataPluginUtil {
  public static void prepareBigDataPlugin( final Path pluginFolder, final String activeHadoopConfig )
    throws IOException, KettlePluginException {
    writeActiveHadoopConfig( pluginFolder, activeHadoopConfig );
    registerPlugin( pluginFolder );
  }

  private static void writeActiveHadoopConfig( final Path pluginFolder, final String activeHadoopConfig )
    throws IOException {
    Properties pluginProperties = new Properties();
    Path pluginPropertiesFile = pluginFolder.resolve( "plugin.properties" );
    try ( BufferedReader reader = newBufferedReader( pluginPropertiesFile, Charset.defaultCharset() ) ) {
      pluginProperties.load( reader );
    }
    pluginProperties.setProperty( "active.hadoop.configuration", activeHadoopConfig );
    try ( BufferedWriter writer = newBufferedWriter( pluginPropertiesFile, Charset.defaultCharset() ) ) {
      pluginProperties.store( writer, "" );
    }
  }

  private static void registerPlugin( final Path pluginFolder ) throws IOException, KettlePluginException {
    final Map<Class<?>, String> classMap = new HashMap<>();
    classMap.put( LifecycleListener.class, "org.pentaho.di.core.hadoop.HadoopSpoonPlugin" );
    classMap.put( GUIOption.class, "org.pentaho.di.core.hadoop.HadoopSpoonPlugin" );

    ArrayList<String> libraries = listLibs( pluginFolder );
    Plugin plugin = new Plugin(
      new String[] { "HadoopSpoonPlugin" }, LifecyclePluginType.class, LifecycleListener.class, "", "HadoopSpoonPlugin",
      "", null, false, false, classMap, libraries, null, pluginFolder.toUri().toURL(), null, null, null );
    PluginRegistry.getInstance().registerPlugin( LifecyclePluginType.class, plugin );
  }

  private static ArrayList<String> listLibs( final Path pluginFolder ) throws IOException {
    ArrayList<String> libraries = new ArrayList<>();
    try ( DirectoryStream<Path> paths = newDirectoryStream( pluginFolder, "*.jar" ) ) {
      for ( Path path : paths ) {
        libraries.add( path.toString() );
      }
    }
    try ( DirectoryStream<Path> paths = newDirectoryStream( pluginFolder.resolve( "lib" ), "*.jar" ) ) {
      for ( Path path : paths ) {
        libraries.add( path.toString() );
      }
    }
    return libraries;
  }
}
