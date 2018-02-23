package org.apache.hadoop.hive.ql.processors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Optional;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.mortbay.log.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
/**
 * Created by kartik.bhatia on 22/02/18.
 */
public class QueueFetcher {
    public static final String SUDO_USER_PROP_KEY = "SUDO_USER";
    public static final String USER = "USER";
    private static Optional<String> queueToBeSet;
    public static ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        queueToBeSet = Optional.absent();
    }

    public static String getQueueForLoggedInUser(FDPGatewayBoxConfiguration fdpGatewayBoxConfiguration) throws IOException, InterruptedException {
        if(queueToBeSet.isPresent()){
            Log.info("Queue has already been fetched once which is {}", queueToBeSet.get());
            return queueToBeSet.get();
        }
        Log.info("Setting queue for the first time!");
        String userId = getLoggedInUser();
        Log.info("Logged in user is {}", userId);
        if(userId!=null){
            String billingOrg = getBillingOrgFromUserID(fdpGatewayBoxConfiguration, userId);
            Log.info("Got billing org {}", billingOrg);
            if(billingOrg==null){
                Log.info("No billing org found for user!, executing in default queue i.e {}", fdpGatewayBoxConfiguration.getAdhocDefaultQueue());
                return fdpGatewayBoxConfiguration.getAdhocDefaultQueue();
            }
            String queueName = getQueueForBillingOrg(fdpGatewayBoxConfiguration, billingOrg);
            Log.info("Got queue name is {}", queueName);
            if(queueName==null){
                Log.info("No queue found for billing org!, executing in default queue i.e {}", fdpGatewayBoxConfiguration.getAdhocDefaultQueue());
                return fdpGatewayBoxConfiguration.getAdhocDefaultQueue();
            }
            return queueName;

        }
        return fdpGatewayBoxConfiguration.getAdhocDefaultQueue();
    }

    private static String getQueueForBillingOrg(FDPGatewayBoxConfiguration fdpGatewayBoxConfiguration, String billingOrg) throws IOException {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(fdpGatewayBoxConfiguration.getIronbankUrl() + billingOrg);
        Log.info("Requesting from {}", request.getURI());
        String result = getResponseString(client, request);
        Log.info("Got response {}", result);
        Map<String, Map<String, String>> billingOrgsQueues = mapper.readValue(result, TypeFactory.defaultInstance().constructType(Map.class));
        if(billingOrgsQueues!=null && billingOrgsQueues.get("priority_to_queue_name_map")!=null){
            return billingOrgsQueues.get("priority_to_queue_name_map").get("3");
        }
        return null;
    }

    private static String getBillingOrgFromUserID(FDPGatewayBoxConfiguration fdpGatewayBoxConfiguration, String userId) throws IOException {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(fdpGatewayBoxConfiguration.getGringottsUrl());
        request.addHeader("x-authenticated-user", userId);
        request.addHeader("X-Client-Id", fdpGatewayBoxConfiguration.getClientId());
        request.addHeader("X-Client-Secret", fdpGatewayBoxConfiguration.getClientSecretKey());
        Log.info("Requesting billing org for user {}, using url {}", userId, request.getURI());
        String result = getResponseString(client, request);
        Log.info("Got response {}", result);
        List<String> billingOrgs = mapper.readValue(result, TypeFactory.defaultInstance().constructType(List.class));
        if(billingOrgs!=null && !billingOrgs.isEmpty()){
            Collections.sort(billingOrgs);
            //Get first org in sorted order till user selecting billing org is not self serve
            return billingOrgs.get(0);
        }
        else {
            Log.info("No billing org found for user!");
            return null;
        }
    }

    private static String getResponseString(CloseableHttpClient client, HttpGet request) throws IOException {
        HttpResponse response = client.execute(request);
        BufferedReader rd = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent()));

        StringBuffer result = new StringBuffer();
        String line = "";
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        return result.toString();
    }

    private static String getLoggedInUser() throws IOException, InterruptedException {
        Log.info("Getting logged in user!");
        Optional<String> sudoUser = Optional.fromNullable(System.getenv().get(SUDO_USER_PROP_KEY));
        Optional<String> user = Optional.fromNullable(System.getenv().get(USER));
        if(sudoUser.isPresent()){
            Log.info("Got prop {}, value {}", SUDO_USER_PROP_KEY, sudoUser.get());
            return sudoUser.get();
        }
        else if(user.isPresent()){
            Log.info("Got prop {}, value {}", USER, user.get());
            return user.get();
        }
        return null;
    }

}
