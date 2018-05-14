package org.apache.hadoop.hive.ql.propertymodifier;

/**
 * Created by kartik.bhatia on 14/05/18.
 */
public enum RequestingIpWrapper {

  INSTANCE {
    @Override
    public String getRequestingIp() {
      return requestingIp.get();
    }

    @Override
    public void setRequestingIp(String requestingIpStr) {
      requestingIp.set(requestingIpStr);
    }
  };

  public ThreadLocal<String> requestingIp;

  public abstract String getRequestingIp();

  public abstract void setRequestingIp(String requestingIp);
}
