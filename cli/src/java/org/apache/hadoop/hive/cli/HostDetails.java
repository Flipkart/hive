package org.apache.hadoop.hive.cli;

import java.net.InetAddress;

/**
 * Created by kartik.bhatia on 14/05/18.
 */
public class HostDetails {
  private final String hostname;
  private final String hostAddress;

  public HostDetails(String hostname, String hostAddress) {
    this.hostname = hostname;
    this.hostAddress = hostAddress;
  }

  public static HostDetails current() {
    try {
      final InetAddress localHost = InetAddress.getLocalHost();
      String hostname = localHost.getHostName();
      String hostAddress = localHost.getHostAddress();

      try {
        Process exec = Runtime.getRuntime().exec("hostname -I");
        exec.waitFor();
        String hostIPAddress = BasicConverters.readInputStreamToString(exec.getInputStream(), 4096);
        if (hostIPAddress != null && !hostIPAddress.isEmpty()) {
          hostAddress = hostname = hostIPAddress.trim();
        }
      } catch (Exception ignored) {
      }

      return new HostDetails(hostname, hostAddress);
    } catch (Exception e) {
      throw new HostDetailsExtractionException(e);
    }
  }

  public String getHostname() {
    return hostname;
  }

  public String getHostAddress() {
    return hostAddress;
  }

  public static class HostDetailsExtractionException extends RuntimeException {
    public HostDetailsExtractionException(Exception e) {
      super(e);
    }
  }

}