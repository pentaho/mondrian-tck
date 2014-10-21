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
