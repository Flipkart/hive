package org.apache.hadoop.hive.ql.propertymodifier;

/**
 * Created by kartik.bhatia on 14/05/18.
 */
public class ContextForJobName {

  private final String currentStage;
  private final String totalJobs;
  private final String username;
  private final String queryId;
  private final String requestingIp;

  public ContextForJobName(String currentStage, String totalJobs,
      String username, String queryId, String requestingIp) {
    this.currentStage = currentStage;
    this.totalJobs = totalJobs;
    this.username = username;
    this.queryId = queryId;
    this.requestingIp = requestingIp;
  }
}
