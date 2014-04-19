package com.richipal.ondeck.util;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Created by rsingh on 4/19/14.
 */
public class PropertiesConfig {

  private static PropertiesConfig instance;
  private PropertiesConfiguration config;

  private PropertiesConfig() throws ConfigurationException{
    synchronized (this) {


      String propertiesFile = "src/main/resources/properties/mappings.properties";

      try {
        config = new PropertiesConfiguration(propertiesFile);
      } catch (Exception e) {
        e.printStackTrace();
        throw new ConfigurationException(
            "Failed to configure properties from file:" + propertiesFile);
      }
    }
  }

  public static PropertiesConfig getInstance() throws RuntimeException {
    if (null == instance) {
      try {
        instance = new PropertiesConfig();
      } catch (ConfigurationException ex) {
        throw new RuntimeException(ex);
      }
    }
    return instance;
  }

  public PropertiesConfiguration getConfigure() {
    return config;
  }

  public void setConfig(PropertiesConfiguration config) {
    this.config = config;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    throw new CloneNotSupportedException();
  }

}
