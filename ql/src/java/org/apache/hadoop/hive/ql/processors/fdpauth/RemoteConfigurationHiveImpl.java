package org.apache.hadoop.hive.ql.processors.fdpauth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.utils.cfg.Configuration;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.util.Map;

import static com.flipkart.fdp.utils.cfg.ConfigConstants.*;

/**
 * Created by kartik.bhatia on 09/03/18.
 */
public class RemoteConfigurationHiveImpl implements Configuration {
    private static final Object KEYS_FIELD = "keys";
    private static final String REQUEST_FORMAT = "http://%s:%d/v1/buckets/%s?version=-1";
    private static final ObjectMapper mapper = new ObjectMapper();
    private final String bucketName;
    private Map<String, Object> keys;

    public RemoteConfigurationHiveImpl(String cfgSvcBucketName) {
        this.bucketName = cfgSvcBucketName;
        this.keys = init();
    }

    public Map<String, Object> init() {
        String cfgSvcHost = System.getProperty(CONFIG_SVC_HOST, DEFAULT_CONFIG_SVC_HOST);
        int cfgSvcPort = Integer.valueOf(System.getProperty(CONFIG_SVC_PORT, DEFAULT_CONFIG_SVC_PORT));
        final Map keysMap;
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(String.format(REQUEST_FORMAT, cfgSvcHost, cfgSvcPort, bucketName));
        String responseFromConfigService;
        try {
            responseFromConfigService = HttpUtils.getResponseString(client, request);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
        final Map map;
        try {
            map = mapper.readValue(responseFromConfigService, Map.class);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
        keysMap = (Map) map.get(KEYS_FIELD);
        return keysMap;
    }

    @Override
    public Map<String, Object> getKeys() {
        return keys;
    }

    @Override
    public Object get(String key) {
        return keys.get(key);
    }
}
