package org.apache.hadoop.hive.metastore;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;

public abstract class SchemeHandler {

    protected final Configuration hiveConf;

    public SchemeHandler(Configuration hiveConf) {
        this.hiveConf = hiveConf;
    }

    public abstract URI getOne(URI uri);
    public abstract String getScheme();
}
