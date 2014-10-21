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
