package org.apache.hadoop.hive.ql.propertymodifier;

/**
 * Created by kartik.bhatia on 14/05/18.
 */
public enum UserNameWrapper {
  INSTANCE {
    @Override
    public String getUsername() {
      return username.get();
    }

    @Override
    public void setUsername(String usrname) {
      username.set(usrname);
    }
  };

  public ThreadLocal<String> username;
  public abstract String getUsername();
  public abstract void setUsername(String usrname);
}
