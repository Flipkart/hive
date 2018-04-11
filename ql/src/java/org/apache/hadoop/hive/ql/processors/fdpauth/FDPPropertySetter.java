package org.apache.hadoop.hive.ql.processors.fdpauth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by kartik.bhatia on 09/03/18.
 */
@Slf4j
public class FDPPropertySetter {
    public static final String MAPRED_QUEUE_PROP = "mapreduce.job.queuename";
    public static final String TEZ_QUEUE_PROP = "tez.queue.name";
    public static final String HIVE_QUERY_NAME = "hive.query.name";
    public static final Logger LOG = LoggerFactory.getLogger(FDPPropertySetter.class);
    public static final SessionState.LogHelper console = new SessionState.LogHelper(LOG);
    public static final String DELIMITER = "---";
    public static final String TOTAL_STAGE_DELIMITER = "/";

    public static void setUserSpecificQueue(HiveConf conf) {
        if (!customisedPropsToBeSet()) return;
        log.info("Setting user specific property for {}", FDPAuth.getInstance().getRequestingIp());
        try {
            setQueue(FDPAuth.getInstance(), conf);
        } catch (BillingOrgNotFoundException e) {
            log.error("Couldn't find billing org, exiting with error");
            throw new RuntimeException(e.getMessage());
        } catch (Throwable e) {
            log.error(e.getMessage());
        }
    }

    public static boolean customisedPropsToBeSet() {
        if (!isUserSpecificPropertiesToBeSetForRequestigIp()) {
            log.info("Request coming from ip {} is a whitelisted ip, not setting any property");
            return false;
        }

        if (!FDPAuth.getInstance().getConfig().isEnabled()) {
            log.info("FDP Auth is not enabled! Normal Execution flow will continue");
            return false;
        }
        return true;
    }

    @VisibleForTesting
    protected static boolean isUserSpecificPropertiesToBeSetForRequestigIp() {
        String requestingIp = FDPAuth.getInstance().getRequestingIp();
        if (FDPAuth.getInstance().getSetOfWhiteListedIps().contains(requestingIp)) {
            return false;
        }
        return true;
    }

    public static void setJobName(FDPAuth fdpAuth, String existingName, HiveConf conf, String stage, int jobs) {
        String queryId = conf.getVar(HiveConf.ConfVars.HIVEQUERYID);
        String loggedInUser = "NOT FOUND";
        try {
            loggedInUser = QueueFetcher.getLoggedInUser(fdpAuth, conf);
        } catch (IOException | InterruptedException e) {
            log.error("Couldn't fetch logged in user due to !" + e.getMessage());
        }
        String customisedName = queryId + DELIMITER + fdpAuth.getRequestingIp() + DELIMITER
                + stage + TOTAL_STAGE_DELIMITER + jobs + DELIMITER + loggedInUser + DELIMITER + existingName;
        log.info("Setting property {} as {}", MRJobConfig.JOB_NAME, customisedName);
        conf.set(MRJobConfig.JOB_NAME, customisedName);
        log.info("Setting property {} as {}", HIVE_QUERY_NAME, customisedName);
        conf.set(HIVE_QUERY_NAME, customisedName);
    }

    private static void setQueue(FDPAuth fdpAuth, HiveConf conf) throws BillingOrgNotFoundException {
        try {
            Optional<String> queueOptional = QueueFetcher.getQueueForLoggedInUser(fdpAuth, conf);
            if (!queueOptional.isPresent()) {
                console.printError(fdpAuth.getConfig().getQueueNotFoundErrorMessage());
                throw new BillingOrgNotFoundException("Couldn't find billing org mapping for user!");
            }
            String queue = queueOptional.get();
            Optional<String> mapredQueueOptional = Optional.fromNullable(conf.get(MAPRED_QUEUE_PROP));
            Optional<String> tezQueueOptional = Optional.fromNullable(conf.get(TEZ_QUEUE_PROP));
            if (!mapredQueueOptional.isPresent() || mapredQueueOptional.get().equals("default")) {
                log.info("Setting mapred queue to {} as it is not set by user", queue);
                conf.set(MAPRED_QUEUE_PROP, queue);
            }
            if (!tezQueueOptional.isPresent()) {
                log.info("Setting TEZ queue to {} as it is not set by user", queue);
                conf.set(TEZ_QUEUE_PROP, queue);
            }
            if (!conf.get(MAPRED_QUEUE_PROP).equals(queue)) {
                console.printError(String.format("You have set invalid queue name %s for mapred job, this will be ignored and set to apt org queue %s",
                        conf.get(MAPRED_QUEUE_PROP), queue));
                conf.set(MAPRED_QUEUE_PROP, queue);
            }
            if (!conf.get(TEZ_QUEUE_PROP).equals(queue)) {
                console.printError(String.format("You have set invalid queue name %s for tez job, this will be ignored and set to apt org queue %s",
                        conf.get(TEZ_QUEUE_PROP), queue));
                conf.set(TEZ_QUEUE_PROP, queue);
            }
        } catch (IOException e) {
            log.info("Fetching of queue failed due to {}", e.getMessage());
        } catch (InterruptedException e) {
            log.info("Fetching of queue failed due to {}", e.getMessage());
        }
    }

    public static class BillingOrgNotFoundException extends RuntimeException {

        public BillingOrgNotFoundException(String message) {
            super(message);
        }
    }

}
