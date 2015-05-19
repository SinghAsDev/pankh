package com.singhasdev.pankh.analysis;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class Context {
  private Properties prop;

  public Context(String file) throws Exception {
    prop = new Properties();
    InputStream is = new FileInputStream(file);
    prop.load(is);
  }

  public String getString(String key) {
    return prop.getProperty(key);
  }
}