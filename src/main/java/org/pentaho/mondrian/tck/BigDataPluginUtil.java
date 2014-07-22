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

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class BigDataPluginUtil {

  private static final FilenameFilter JAR_FILTER = new FilenameFilter() {
    @Override public boolean accept( final File dir, final String name ) {
      return name.endsWith( ".jar" );
    }
  };

  public static void prepareBigDataPlugin( final File pluginFolder, final String activeHadoopConfig )
    throws IOException, KettlePluginException {
    writeActiveHadoopConfig( pluginFolder, activeHadoopConfig );
    registerPlugin( pluginFolder );
  }

  private static void writeActiveHadoopConfig( final File pluginFolder, final String activeHadoopConfig )
    throws IOException {
    Properties pluginProperties = new Properties();
    File pluginPropertiesFile = new File( pluginFolder, "plugin.properties" );
    FileReader reader = new FileReader( pluginPropertiesFile );
    pluginProperties.load( reader );
    reader.close();
    pluginProperties.setProperty( "active.hadoop.configuration", activeHadoopConfig );
    FileWriter writer = new FileWriter( pluginPropertiesFile, false );
    pluginProperties.store( writer, "" );
    writer.close();
  }

  private static void registerPlugin( final File pluginFolder ) throws MalformedURLException, KettlePluginException {
    final Map<Class<?>, String> classMap = new HashMap<>();
    classMap.put( LifecycleListener.class, "org.pentaho.di.core.hadoop.HadoopSpoonPlugin" );
    classMap.put( GUIOption.class, "org.pentaho.di.core.hadoop.HadoopSpoonPlugin" );

    ArrayList<String> libraries = listLibs( pluginFolder );
    Plugin plugin = new Plugin(
      new String[] { "HadoopSpoonPlugin" }, LifecyclePluginType.class, LifecycleListener.class, "", "HadoopSpoonPlugin",
      "", null, false, false, classMap, libraries, null, pluginFolder.toURI().toURL(), null, null, null );
    PluginRegistry.getInstance().registerPlugin( LifecyclePluginType.class, plugin );
  }

  private static ArrayList<String> listLibs( final File pluginFolder ) {
    ArrayList<String> libraries = new ArrayList<>();
    String[] topLibs = pluginFolder.list( JAR_FILTER );
    for ( String topLib : topLibs ) {
      libraries.add( pluginFolder.getAbsolutePath() + "/" + topLib );
    }
    File pluginLib = new File( pluginFolder, "lib" );
    String[] libs = pluginLib.list( JAR_FILTER );
    for ( String lib : libs ) {
      libraries.add( pluginLib.getAbsolutePath() + "/" + lib );
    }
    return libraries;
  }
}
