package org.apache.hadoop.hive.ql.propertymodifier;

public class NoQueueEnforced implements QueueEnforcer {

    @Override
    public String getEnforcedQueue(String existingQueue) {
        return existingQueue;
    }
}
