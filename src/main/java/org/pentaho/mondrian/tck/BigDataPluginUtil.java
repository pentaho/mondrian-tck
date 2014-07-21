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
