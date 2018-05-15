package org.apache.hadoop.hive.ql.propertymodifier;

import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Created by kartik.bhatia on 14/05/18.
 */
public interface QueueEnforcer {
    String getEnforcedQueue(String existingQueue, String initiator, HiveConf conf);
}
