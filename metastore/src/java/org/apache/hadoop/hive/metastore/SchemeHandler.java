package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.conf.HiveConf;

import java.net.URI;

public abstract class SchemeHandler {

  protected final HiveConf hiveConf;

  public SchemeHandler(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  public abstract URI getOne(URI uri);
  public abstract String getScheme();
}
