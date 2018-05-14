package org.apache.hadoop.hive.ql.propertymodifier;

public class ReturnExistingQueue implements QueueEnforcer {

    @Override
    public String getEnforcedQueue(String existingQueue, String initiator) {
        return existingQueue;
    }
}
