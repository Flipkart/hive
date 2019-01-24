package org.apache.hive.beeline;

import com.google.common.base.Optional;

import java.io.IOException;

public class ExtractShellUtils
{

  public static final String SUDO_USER_PROP_KEY = "SUDO_USER";
  public static final String USER = "USER";

  public static String getLoggedInUserFromShell() {
    Optional<String> sudoUser = Optional.fromNullable(System.getenv().get(SUDO_USER_PROP_KEY));
    if (sudoUser.isPresent()) {
      return sudoUser.get();
    }
    Optional<String> user = Optional.fromNullable(System.getenv().get(USER));
    if (user.isPresent()) {
      return user.get();
    }
    throw new RuntimeException("Unable to identify User");
  }
}
