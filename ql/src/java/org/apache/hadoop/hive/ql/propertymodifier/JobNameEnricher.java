package org.apache.hadoop.hive.ql.propertymodifier;


/**
 * Created by kartik.bhatia on 14/05/18.
 */
public interface JobNameEnricher {
    String getEnrichedJobName(String existingJobName, ContextForJobName context);
}
