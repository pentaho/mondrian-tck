package org.pentaho.mondrian.tck;

import org.eigenbase.util.property.Property;

import java.util.HashMap;
import java.util.Map;

public class PropertyContext {
  private Map<Property, String> properties = new HashMap<>();

  public PropertyContext withProperty( Property property, String value ) {
    properties.put( property, value );
    return this;
  }

  public void execute( Runnable runnable ) {
    Map<Property, String> oldValues = set( properties );
    runnable.run();
    set( oldValues );
  }

  private Map<Property, String> set( final Map<Property, String> newValues ) {
    Map<Property, String> oldValues = new HashMap<>();
    for ( Property property : newValues.keySet() ) {
      oldValues.put( property, property.getString() );
      property.setString( newValues.get( property ) );
    }
    return oldValues;
  }
}
