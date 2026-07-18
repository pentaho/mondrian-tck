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
    try {
      runnable.run();
    } finally {
      set( oldValues );
    }
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
