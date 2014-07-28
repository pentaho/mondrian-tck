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
