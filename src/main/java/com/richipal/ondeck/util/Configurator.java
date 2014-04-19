package com.richipal.ondeck.util;


import org.apache.commons.configuration.ConfigurationException;

import java.util.ArrayList;
import java.util.Collections;


/**
 * Created by rsingh on 4/19/14.
 */
public class Configurator {

  static boolean isAlreadyAsserted = false;
  
  public static void assertApplicationEnv() throws ConfigurationException {
    if (isAlreadyAsserted) {
      return;
    }

    if (System.getenv().get("JAVA_HOME") == null) {
      throw new ConfigurationException(
          "Need JAVA_HOME set in the environment.");
    }

    isAlreadyAsserted = true;
  }


  public static boolean hasProperty(String prop) throws ConfigurationException {

    return (PropertiesConfig.getInstance().getConfigure().getString(prop)
        == null)
            ? false
            : true;
  }

  /** Retrieve single values */
  public static String getProperty(String prop) throws ConfigurationException {

    String val = PropertiesConfig.getInstance().getConfigure().getString(prop);

    if (val == null) {
      throw new ConfigurationException(
          "Configurator couldn't find property:[" + prop + "]");
    }
    return val;
  }

  /** Retrieve list values */
  public static ArrayList<String> getList(String prop) throws ConfigurationException {

    // Throw Exception if it does not exist
    getProperty(prop);

    // Otherwise, coerce string into list and return...
    
    String[] vals = PropertiesConfig.getInstance().getConfigure().getStringArray(
        prop);

    if (vals == null) {
      throw new ConfigurationException(
          "Configurator couldn't find property:[" + prop + "]");
    }

    ArrayList<String> list = new ArrayList<String>();

    Collections.addAll(list, vals);
    return list;  
  }


}

