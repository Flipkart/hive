package org.apache.hadoop.hive.ql.propertymodifier;

import org.apache.hadoop.hive.conf.HiveConf;

public class ReturnExistingQueue implements QueueEnforcer {

  @Override
  public String getEnforcedQueue(String existingQueue, String initiator, HiveConf conf) {
    return existingQueue;
  }
}
