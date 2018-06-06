package org.apache.hadoop.hive.ql.processors.fdpauth;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.mortbay.log.Log;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by kartik.bhatia on 22/02/18.
 */
public class QueueFetcher {
    public static final String SUDO_USER_PROP_KEY = "SUDO_USER";
    public static final String USER = "USER";
    private static LoadingCache<String, Optional<String>> queueCache;
    public static ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        queueCache = CacheBuilder
                .newBuilder()
                .expireAfterWrite(FDPAuth.getInstance().getConfig().getQueueCacheExpire(), TimeUnit.SECONDS)
                .maximumSize(FDPAuth.getInstance().getConfig().getQueueCacheLimit())
                .build(
                        new CacheLoader<String, Optional<String>>() {
                            @Override
                            public Optional<String> load(String user) throws Exception {
                                return Optional.fromNullable(getQueueForUser(FDPAuth.getInstance(), user));
                            }
                        }
                );
    }

    public static String getLoggedInUser(FDPAuth fdpAuth, HiveConf conf) throws IOException, InterruptedException {
        Optional<String> user;
        if (fdpAuth.getConfig().getBoxType().equals(FDPAuth.GATEWAY)) {
            user = Optional.fromNullable(getLoggedInUserFromShell());
        } else if (fdpAuth.getConfig().getBoxType().equals(FDPAuth.HIVESERVER)) {
            user = getHiveServerSetUser(conf);
        } else {
            throw new RuntimeException("Box is not gateway/hiveserver");
        }
        if (!user.isPresent()) {
            throw new RuntimeException("No user set for the query");
        }
        return user.get();
    }

    public static Optional<String> getQueueForLoggedInUser(FDPAuth fdpAuth, HiveConf conf) throws IOException, InterruptedException {
        return getCachedQueueForUser(getLoggedInUser(fdpAuth, conf));
    }

    private static Optional<String> getCachedQueueForUser(String loggedInUser) {
        return queueCache.getUnchecked(loggedInUser);
    }

    private static Optional<String> getHiveServerSetUser(HiveConf conf) throws IOException {
        return Optional.fromNullable(conf.getUser());
    }

    public static String getQueueForUser(FDPAuth fdpAuth, String user) throws IOException, InterruptedException {
        Log.info("Setting queue for the first time!");
        String userId = user;
        Log.info("Logged in user is {}", userId);
        if (userId != null) {
            String billingOrg = getBillingOrgFromUserID(fdpAuth, userId);
            Log.info("Got billing org {}", billingOrg);
            if (billingOrg == null) {
                Log.info("No billing org found for user {}!, exiting with error", userId);
                return null;
            }
            String queueName = getQueueForBillingOrg(fdpAuth, billingOrg);
            Log.info("Got queue name is {}", queueName);
            if (queueName == null) {
                Log.warn("No queue found for billing org!, executing in default queue i.e {}", fdpAuth.getConfig().getAdhocDefaultQueue());
                return fdpAuth.getConfig().getAdhocDefaultQueue();
            }
            return queueName;

        }
        return fdpAuth.getConfig().getAdhocDefaultQueue();
    }

    private static String getQueueForBillingOrg(FDPAuth fdpAuth, String billingOrg) throws IOException {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(fdpAuth.getConfig().getIronbankUrl() + billingOrg);
        Log.info("Requesting from {}", request.getURI());
        String result = HttpUtils.getResponseString(client, request);
        Log.info("Got response {}", result);
        Map<String, Map<String, String>> billingOrgsQueues = mapper.readValue(result, TypeFactory.defaultInstance().constructType(Map.class));
        if (billingOrgsQueues != null && billingOrgsQueues.get("priority_to_queue_name_map") != null) {
            return billingOrgsQueues.get("priority_to_queue_name_map").get("3");
        }
        return null;
    }

    private static String getBillingOrgFromUserID(FDPAuth fdpAuth, String userId) throws IOException {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(fdpAuth.getConfig().getGringottsUrl());
        request.addHeader("x-authenticated-user", userId);
        request.addHeader("X-Client-Id", fdpAuth.getConfig().getClientId());
        request.addHeader("X-Client-Secret", fdpAuth.getConfig().getClientSecretKey());
        Log.info("Requesting billing org for user {}, using url {}", userId, request.getURI());
        String result = HttpUtils.getResponseString(client, request);
        Log.info("Got response {}", result);
        List<String> billingOrgs = mapper.readValue(result, TypeFactory.defaultInstance().constructType(List.class));
        if (billingOrgs != null && !billingOrgs.isEmpty()) {
            Collections.sort(billingOrgs);
            //Get first org in sorted order till user selecting billing org is not self serve
            return billingOrgs.get(0);
        } else {
            Log.info("No billing org found for user!");
            return null;
        }
    }

    public static String getLoggedInUserFromShell() throws IOException, InterruptedException {
        Log.info("Getting logged in user!");
        Optional<String> sudoUser = Optional.fromNullable(System.getenv().get(SUDO_USER_PROP_KEY));
        Optional<String> user = Optional.fromNullable(System.getenv().get(USER));
        if (sudoUser.isPresent()) {
            Log.info("Got prop {}, value {}", SUDO_USER_PROP_KEY, sudoUser.get());
            return sudoUser.get();
        } else if (user.isPresent()) {
            Log.info("Got prop {}, value {}", USER, user.get());
            return user.get();
        }
        return null;
    }

}
