package org.apache.hadoop.hive.ql.processors.fdpauth;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.utils.cfg.ConfigService;
import com.flipkart.fdp.utils.cfg.ConfigServiceImpl;
import com.flipkart.fdp.utils.cfg.Configuration;
import com.flipkart.fdp.utils.cfg.KeyProviderFactory;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by kartik.bhatia on 08/03/18.
 */
@Slf4j
@Getter
@Setter
public class FDPAuth {
    public static final String BUCKET_FILE = "/etc/default/fdp-hive-guardrails";
//    public static final String BUCKET_FILE = "/Users/kartik.bhatia/work/hive/ql/src/test/org/apache/hadoop/hive/ql/processors/fdpauth/bucketfile";
    public static final String GATEWAY = "GATEWAY";
    public static final String HIVESERVER = "HIVESERVER";
    private ThreadLocal<String> currentIp = new ThreadLocal<>();
    private static AtomicReference<FDPAuth> fdpAuthAtomicRef = new AtomicReference<>(null);
    private FDPAuthConfig config;
    private static Refresher refresher;
    private static Thread refresherThread;

    static {
        refreshInstance();
        log.info("Starting refresher thread!");
        refresher = new Refresher();
        refresherThread = new Thread(refresher);
        refresherThread.setName("refresher thread");
        refresherThread.start();
        log.info("Started refresher thread!");
    }

    private FDPAuth() {
    }


    private static void refreshInstance() {
        FDPAuth fdpAuth = new FDPAuth();
        fdpAuth.setConfig(getConfigService(BUCKET_FILE).getConfig(FDPAuthConfig.class));
        fdpAuthAtomicRef.set(fdpAuth);
        log.info("FdpAuth instance is {}", fdpAuthAtomicRef.get());
    }

    public static FDPAuth getInstance() {
        return fdpAuthAtomicRef.get();
    }

    public static ConfigService getConfigService(String bucketFileLocation) {
        final String bucketName = getBucketName(bucketFileLocation);
        Configuration keyProvider;
        keyProvider = getAptKeyProvider(bucketName);
        final ConfigService configService = new ConfigServiceImpl(keyProvider);
        log.info("Config obtained successfully from : " + bucketName);
        return configService;
    }

    private static Configuration getAptKeyProvider(String bucketName) {
        Configuration keyProvider;
        if (isFile(bucketName)) {
            keyProvider = KeyProviderFactory.fileKeyProvider(bucketName);
        } else {
            keyProvider = new RemoteConfigurationHiveImpl(bucketName);
        }
        return keyProvider;
    }

    private static boolean isFile(String cfgSvcBucketName) {
        return new File(cfgSvcBucketName).isFile();
    }

    private static String getBucketName(String bucketFileLocation) {
        File propFile = new File(bucketFileLocation);
        if (propFile.exists() && !propFile.isDirectory()) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                return mapper.readValue(propFile, String.class);
            } catch (IOException e) {
                throw new RuntimeException(String.format("Couldn't parse prop file at %s", BUCKET_FILE));
            }
        }
        throw new RuntimeException(String.format("Bucket file at %s is not there for guardrails!", BUCKET_FILE));
    }

    public static Set<String> getSetOfWhiteListedIps() {
        return Sets.newHashSet(fdpAuthAtomicRef.get().getConfig().getWhiteListedIps().split(","));
    }

    public String getRequestingIp() {
        return currentIp.get();
    }

    public void setCurrentIp(String currentIpToBeSet) {
        currentIp.set(currentIpToBeSet);
    }


    public static void main(String[] args) throws InterruptedException {
        FDPAuth instance = FDPAuth.getInstance();
        while (true) {

            System.out.println(instance);
            System.out.println(FDPAuth.getInstance());
        }
    }

    @Override
    public String toString() {
        return "FDPAuth{" +
                "currentIp=" + currentIp +
                ", config=" + config +
                '}';
    }

    public static class Refresher implements Runnable {

        //Keeping refresh interval as 2 minutes
        public static final int DEFAULT_REFRESH_INTERVAL = 120000;

        @Override
        public void run() {
            while(true) {
                try {
                    Thread.sleep(getRefreshInterval());
                    log.info("Refreshing fdp auth instance");
                    refreshInstance();
                    log.info("fdp auth refreshed!");
                } catch (Throwable e) {
                    log.error("Couldn't refresh auth instance! Error {}", e.getMessage());
                }
            }

        }

        private int getRefreshInterval() {
            Integer refreshInterval = DEFAULT_REFRESH_INTERVAL;
            try {
                String fileName = "/etc/default/refreshInterval";
                String line;
                FileReader fileReader =
                        new FileReader(fileName);

                BufferedReader bufferedReader =
                        new BufferedReader(fileReader);

                while((line = bufferedReader.readLine()) != null) {
                    refreshInterval = Integer.valueOf(line);
                }
                bufferedReader.close();
            } catch (Throwable e) {
                log.error("Couldn't get custom refresh interval, falling back to default due to {}", e.getMessage());
            }
            return refreshInterval;
        }
    }
}