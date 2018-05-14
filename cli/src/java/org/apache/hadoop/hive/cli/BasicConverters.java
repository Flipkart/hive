package org.apache.hadoop.hive.cli;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * Created by kartik.bhatia on 14/05/18.
 */
public class BasicConverters {

  public static String readInputStreamToString(final InputStream is, final int bufferSize) {
    final char[] buffer = new char[bufferSize];
    final StringBuilder out = new StringBuilder();
    try (Reader in = new InputStreamReader(is, "UTF-8")) {
      for (; ; ) {
        int rsz = in.read(buffer, 0, buffer.length);
        if (rsz < 0) {
          break;
        }
        out.append(buffer, 0, rsz);
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    return out.toString();
  }
}