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
    public static final String GRINGOTTS_URL = "http://10.47.6.66/billingOrg/user";
    public static final String CLIENT_ID = "QAAS";
    public static final String CLIENT_SECRET_KEY = "423de2b0-cc97-439d-a3f9-673e76d7bbea";
    public static final String IRONBANK_URL = "http://10.47.6.114:11111/queue/";
    public static final String ADHOC_DEFAULT = "adhoc";
    public static final String SSH_TTY_PROP_KEY = "SSH_TTY";
    private static Optional<String> queueToBeSet;
    public static ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        queueToBeSet = Optional.absent();
    }

    public static String getQueueForLoggedInUser() throws IOException, InterruptedException {
        if(queueToBeSet.isPresent()){
            Log.info("Queue ha already been fetched once which is {}", queueToBeSet.get());
            return queueToBeSet.get();
        }
        Log.info("Setting queue for the first time!");
        String userId = getLoggedInUser();
        Log.info("Logged in user is {}", userId);
        if(userId!=null){
            String billingOrg = getBillingOrgFromUserID(userId);
            Log.info("Got billing org {}", billingOrg);
            if(billingOrg==null){
                Log.info("No billing org found for user!, executing in default queue i.e {}", ADHOC_DEFAULT);
                return ADHOC_DEFAULT;
            }
            String queueName = getQueueForBillingOrg(billingOrg);
            Log.info("Got queue name is {}", queueName);
            if(queueName==null){
                Log.info("No queue found for billing org!, executing in default queue i.e {}", ADHOC_DEFAULT);
                return ADHOC_DEFAULT;
            }
            return queueName;

        }
        return ADHOC_DEFAULT;
    }

    private static String getQueueForBillingOrg(String billingOrg) throws IOException {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(IRONBANK_URL + billingOrg);
        Log.info("Requesting from {}", request.getURI());
        HttpResponse response = client.execute(request);
        BufferedReader rd = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent()));

        StringBuffer result = new StringBuffer();
        String line = "";
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        Log.info("Got response {}", result);
        Map<String, Map<String, String>> billingOrgsQueues = mapper.readValue(result.toString(), TypeFactory.defaultInstance().constructType(Map.class));
        if(billingOrgsQueues!=null && billingOrgsQueues.get("priority_to_queue_name_map")!=null){
            return billingOrgsQueues.get("priority_to_queue_name_map").get("3");
        }
        return null;
    }

    private static String getBillingOrgFromUserID(String userId) throws IOException {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(GRINGOTTS_URL);
        request.addHeader("x-authenticated-user", userId);
        request.addHeader("X-Client-Id", CLIENT_ID);
        request.addHeader("X-Client-Secret", CLIENT_SECRET_KEY);
        Log.info("Requesting billing org for user {}, using url {}", userId, request.getURI());
        HttpResponse response = client.execute(request);
        BufferedReader rd = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent()));

        StringBuffer result = new StringBuffer();
        String line = "";
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        Log.info("Got response {}", result);
        List<String> billingOrgs = mapper.readValue(result.toString(), TypeFactory.defaultInstance().constructType(List.class));
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

    private static String getLoggedInUser() throws IOException, InterruptedException {
        Log.info("Getting logged in user!");
        Optional<String> patternOptional = getSshTerminalHackery();
        if(!patternOptional.isPresent()){
            Log.warn("Didn't get pattern to serach, returning null!");
            return null;
        }
        Log.info("Pattern to search is {}", patternOptional.get());
        String[] cmd = {
                "/bin/sh",
                "-c",
                "who | grep -w " + patternOptional.get()
        };
        Process process = Runtime.getRuntime().exec(cmd);
        process.waitFor();

        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        while ((line = reader.readLine())!= null) {
            //who am i command's o/p first column is the actual user
            return line.split(" ")[0];
        }
        return line;
    }

    private static Optional<String> getSshTerminalHackery() {
        Optional<String> terminalNumber = Optional.fromNullable(System.getenv().get(SSH_TTY_PROP_KEY));
        if(!terminalNumber.isPresent()){
            return Optional.absent();
        }
        Log.info("Getting prop {} which is {}", SSH_TTY_PROP_KEY, terminalNumber.get());
        StringBuilder patternToSearch = new StringBuilder();
        String [] splitOpSshTty = terminalNumber.get().split("/");
        int sshttylen = splitOpSshTty.length;
        if(sshttylen >= 2){
            patternToSearch.append(splitOpSshTty[sshttylen-2]);
            patternToSearch.append("/");
            patternToSearch.append(splitOpSshTty[sshttylen-1]);
            return Optional.of(patternToSearch.toString());
        }
        return Optional.absent();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println(getLoggedInUser());
    }
}
