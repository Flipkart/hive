package org.apache.hadoop.hive.ql.propertymodifier;

/**
 * Created by kartik.bhatia on 14/05/18.
 */
public class NoEnrichment implements JobNameEnricher {

  @Override
  public String getEnrichedJobName(String existingJobName, ContextForJobName context) {
    return existingJobName;
  }
}
