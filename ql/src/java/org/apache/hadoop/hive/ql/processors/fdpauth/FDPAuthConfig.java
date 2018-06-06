package org.apache.hadoop.hive.ql.processors.fdpauth;

import com.flipkart.fdp.utils.cfg.ConfigBucketKey;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by kartik.bhatia on 09/03/18.
 */
@Getter
@Setter
@ConfigBucketKey(name = "fdpauth")
public class FDPAuthConfig {
    private String boxType;
    private String whiteListedIps;
    private boolean enabled;
    private String gringottsUrl;
    private String ironbankUrl;
    private String clientId;
    private String clientSecretKey;
    private String adhocDefaultQueue;
    private int queueCacheExpire;
    private int queueCacheLimit;
    private String queueNotFoundErrorMessage;

    @Override
    public String toString() {
        return "FDPAuthConfig{" +
                "boxType='" + boxType + '\'' +
                ", whiteListedIps='" + whiteListedIps + '\'' +
                ", enabled=" + enabled +
                ", gringottsUrl='" + gringottsUrl + '\'' +
                ", ironbankUrl='" + ironbankUrl + '\'' +
                ", clientId='" + clientId + '\'' +
                ", clientSecretKey='" + clientSecretKey + '\'' +
                ", adhocDefaultQueue='" + adhocDefaultQueue + '\'' +
                ", queueCacheExpire=" + queueCacheExpire +
                ", queueCacheLimit=" + queueCacheLimit +
                ", queueNotFoundErrorMessage='" + queueNotFoundErrorMessage + '\'' +
                '}';
    }
}