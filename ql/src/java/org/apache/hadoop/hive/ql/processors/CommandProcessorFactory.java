/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.processors;

import static org.apache.commons.lang.StringUtils.isBlank;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * CommandProcessorFactory.
 *
 */
public final class CommandProcessorFactory {

  public static final String PROPERTIES_FILE_LOCATION = "/etc/default/fdp-properties.json";
  public static final String MAPRED_QUEUE_PROP = "mapreduce.job.queuename";
  public static final String TEZ_QUEUE_PROP = "tez.queue.name";
  public static final String GATEWAY = "gateway";

  public static final Logger LOG = LoggerFactory.getLogger(CommandProcessorFactory.class);
  public static final SessionState.LogHelper console = new SessionState.LogHelper(LOG);
  public static final String MAPRED_JOB_NAME = "mapred.job.name";
  public static final String HIVE_QUERY_NAME = "hive.query.name";

  private CommandProcessorFactory() {
    // prevent instantiation
  }

  private static final Map<HiveConf, Driver> mapDrivers = Collections.synchronizedMap(new HashMap<HiveConf, Driver>());

  public static CommandProcessor get(String cmd)
      throws SQLException {
    return get(new String[]{cmd}, null);
  }

  public static CommandProcessor getForHiveCommand(String[] cmd, HiveConf conf)
    throws SQLException {
    return getForHiveCommandInternal(cmd, conf, false);
  }

  public static CommandProcessor getForHiveCommandInternal(String[] cmd, HiveConf conf,
                                                           boolean testOnly)
    throws SQLException {
    HiveCommand hiveCommand = HiveCommand.find(cmd, testOnly);
    if (hiveCommand == null || isBlank(cmd[0])) {
      return null;
    }
    if (conf == null) {
      conf = new HiveConf();
    }
    Set<String> availableCommands = new HashSet<String>();
    for (String availableCommand : conf.getVar(HiveConf.ConfVars.HIVE_SECURITY_COMMAND_WHITELIST)
      .split(",")) {
      availableCommands.add(availableCommand.toLowerCase().trim());
    }
    if (!availableCommands.contains(cmd[0].trim().toLowerCase())) {
      throw new SQLException("Insufficient privileges to execute " + cmd[0], "42000");
    }
    if (cmd.length > 1 && "reload".equalsIgnoreCase(cmd[0])
      && "function".equalsIgnoreCase(cmd[1])) {
      // special handling for SQL "reload function"
      return null;
    }
    switch (hiveCommand) {
      case SET:
        return new SetProcessor();
      case RESET:
        return new ResetProcessor();
      case DFS:
        SessionState ss = SessionState.get();
        return new DfsProcessor(ss.getConf());
      case ADD:
        return new AddResourceProcessor();
      case LIST:
        return new ListResourceProcessor();
      case DELETE:
        return new DeleteResourceProcessor();
      case COMPILE:
        return new CompileProcessor();
      case RELOAD:
        return new ReloadProcessor();
      case CRYPTO:
        try {
          return new CryptoProcessor(SessionState.get().getHdfsEncryptionShim(), conf);
        } catch (HiveException e) {
          throw new SQLException("Fail to start the command processor due to the exception: ", e);
        }
      default:
        throw new AssertionError("Unknown HiveCommand " + hiveCommand);
    }
  }


  public static CommandProcessor get(String[] cmd, HiveConf conf)
      throws SQLException {
    CommandProcessor result = getForHiveCommand(cmd, conf);
    if (result != null) {
      return result;
    }
    if (isBlank(cmd[0])) {
      return null;
    } else {
      if (conf == null) {
        return new Driver();
      }
      LOG.info("Beginning to set user level properties!");
      setUserSpecificProperties(conf);
      Driver drv = mapDrivers.get(conf);
      if (drv == null) {
        drv = new Driver();
        mapDrivers.put(conf, drv);
      } else {
        drv.resetQueryState();
      }
      drv.init();
      return drv;
    }
  }

  private static void setUserSpecificProperties(HiveConf conf) {
    File propFile = new File(PROPERTIES_FILE_LOCATION);
    if(propFile.exists() && !propFile.isDirectory()){
      try {
        LOG.info("Found file at {}, fetching map from it", PROPERTIES_FILE_LOCATION);
        FDPGatewayBoxConfiguration fdpConfiguration = getMapFromFile(propFile);
        LOG.info("File fetch from {} succeeded with properties", PROPERTIES_FILE_LOCATION, fdpConfiguration);
        if(fdpConfiguration.getBoxType().equals(GATEWAY)){
          LOG.info("Found box type to be {}, proceeding with setting of properties", fdpConfiguration.getBoxType());
          setPropertiesHelper(fdpConfiguration, conf);
        }
        else{
          LOG.info("This is not gateway box, setting nothing!");
          return;
        }
      } catch (BillingOrgNotFoundException billingOrgNotException){
        LOG.info("Couldn't find billing org, exiting with error");
        throw new RuntimeException(billingOrgNotException.getMessage());
      } catch (Throwable e) {
        LOG.error(e.getMessage());
      }
    }
    else {
      LOG.info("File not found at {} to set properties! Continuing normal execution", PROPERTIES_FILE_LOCATION);
    }
  }

  private static void setPropertiesHelper(FDPGatewayBoxConfiguration fdpGatewayBoxConfiguration, HiveConf conf) throws BillingOrgNotFoundException, IOException, InterruptedException {
    setQueue(fdpGatewayBoxConfiguration, conf);
    setJobName(conf);
  }

  private static void setJobName(HiveConf conf) throws IOException, InterruptedException {
    Optional<String> mapredJobNameOptional = Optional.fromNullable(conf.get(MAPRED_JOB_NAME));
    Optional<String> hiveQueryNameOptional = Optional.fromNullable(conf.get(HIVE_QUERY_NAME));
    String loggedInuser = QueueFetcher.getLoggedInUser();
    String mapredJobName = loggedInuser, hiveQueryName = loggedInuser;
    if(mapredJobNameOptional.isPresent()){
      LOG.info("Found prop {} to be set as {}, appending username {}", MAPRED_JOB_NAME, mapredJobNameOptional.get(), loggedInuser);
      mapredJobName = checkExistingNameAndAppendUserIfNotAppended(mapredJobNameOptional.get(), loggedInuser);

    }
    if(hiveQueryNameOptional.isPresent()){
      LOG.info("Found prop {} to be set as {}, appending username {}", HIVE_QUERY_NAME, mapredJobNameOptional.get(), loggedInuser);
      hiveQueryName = checkExistingNameAndAppendUserIfNotAppended(hiveQueryNameOptional.get(), loggedInuser);
    }
    LOG.info("Setting property {} as {}", MAPRED_JOB_NAME, mapredJobName);
    conf.set(MAPRED_JOB_NAME, mapredJobName);
    LOG.info("Setting property {} as {}", HIVE_QUERY_NAME, hiveQueryName);
    conf.set(HIVE_QUERY_NAME, hiveQueryName);
  }

  /**
   * Since multiple commands can be used in session we need to check if we have already set the logged in user name
   * @param existingPropertyVal
   * @param loggedInuser
   * @return
     */
  private static String checkExistingNameAndAppendUserIfNotAppended(String existingPropertyVal, String loggedInuser) {
    String[] splitOfJobName = existingPropertyVal.split("-");
    if(splitOfJobName[splitOfJobName.length -1].equals(loggedInuser)){
      return existingPropertyVal;
    }
    else {
      return existingPropertyVal + "-" + loggedInuser;
    }
  }

  private static void setQueue(FDPGatewayBoxConfiguration fdpGatewayBoxConfiguration, HiveConf conf) throws BillingOrgNotFoundException {
    try {
      String queue = QueueFetcher.getQueueForLoggedInUser(fdpGatewayBoxConfiguration);
      if(queue==null){
        console.printError(fdpGatewayBoxConfiguration.getErrorMsg());
        throw new BillingOrgNotFoundException("Couldn't find billing org mapping for user!");
      }
      Optional<String> mapredQueueOptional = Optional.fromNullable(conf.get(MAPRED_QUEUE_PROP));
      Optional<String> tezQueueOptional = Optional.fromNullable(conf.get(TEZ_QUEUE_PROP));
      if(!mapredQueueOptional.isPresent() || mapredQueueOptional.get().equals("default")){
        LOG.info("Setting mapred queue to {} as it is not set by user", queue);
        conf.set(MAPRED_QUEUE_PROP, queue);
      }
      if(!tezQueueOptional.isPresent()){
        LOG.info("Setting TEZ queue to {} as it is not set by user", queue);
        conf.set(TEZ_QUEUE_PROP, queue);
      }
      if(!conf.get(MAPRED_QUEUE_PROP).equals(queue)){
        console.printError(String.format("You have set invalid queue name %s for mapred job, this will be ignored and set to apt org queue %s",
                conf.get(MAPRED_QUEUE_PROP), queue));
        conf.set(MAPRED_QUEUE_PROP, queue);
      }
      if(!conf.get(TEZ_QUEUE_PROP).equals(queue)){
        console.printError(String.format("You have set invalid queue name %s for tez job, this will be ignored and set to apt org queue %s",
                conf.get(TEZ_QUEUE_PROP), queue));
        conf.set(TEZ_QUEUE_PROP, queue);
      }
    } catch (IOException e) {
      LOG.info("Fetching of queue failed due to {}", e.getMessage());
    } catch (InterruptedException e) {
      LOG.info("Fetching of queue failed due to {}", e.getMessage());
    }
  }

  private static FDPGatewayBoxConfiguration getMapFromFile(File propFile) throws IOException {
    return QueueFetcher.mapper.readValue(propFile, FDPGatewayBoxConfiguration.class);
  }

  public static void clean(HiveConf conf) {
    Driver drv = mapDrivers.get(conf);
    if (drv != null) {
      drv.destroy();
    }

    mapDrivers.remove(conf);
  }

  private static class BillingOrgNotFoundException extends Exception {

    public BillingOrgNotFoundException(String message) {
      super(message);
    }
  }
}
