package org.apache.hadoop.hive.cli;

import com.google.common.base.Optional;
import java.io.IOException;

/**
 * Created by kartik.bhatia on 14/05/18.
 */
public class ShellUserFetcher {

  public static final String SUDO_USER_PROP_KEY = "SUDO_USER";
  public static final String USER = "USER";

  public static String getLoggedInUserFromShell() throws IOException, InterruptedException {
    Optional<String> sudoUser = Optional.fromNullable(System.getenv().get(SUDO_USER_PROP_KEY));
    Optional<String> user = Optional.fromNullable(System.getenv().get(USER));
    if (sudoUser.isPresent()) {
      return sudoUser.get();
    } else if (user.isPresent()) {
      return user.get();
    }
    return null;
  }
}
