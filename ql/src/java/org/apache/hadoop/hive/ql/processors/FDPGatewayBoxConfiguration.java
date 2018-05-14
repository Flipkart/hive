package org.apache.hadoop.hive.ql.processors;

/**
 * Created by kartik.bhatia on 23/02/18.
 */
public class FDPGatewayBoxConfiguration {

    private String boxType;
    private String gringottsUrl;
    private String ironbankUrl;
    private String clientId;
    private String clientSecretKey;
    private String adhocDefaultQueue;

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    private String errorMsg;

    public String getBoxType() {
        return boxType;
    }

    public void setBoxType(String boxType) {
        this.boxType = boxType;
    }

    public String getGringottsUrl() {
        return gringottsUrl;
    }

    public void setGringottsUrl(String gringottsUrl) {
        this.gringottsUrl = gringottsUrl;
    }

    public String getIronbankUrl() {
        return ironbankUrl;
    }

    public void setIronbankUrl(String ironbankUrl) {
        this.ironbankUrl = ironbankUrl;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientSecretKey() {
        return clientSecretKey;
    }

    public void setClientSecretKey(String clientSecretKey) {
        this.clientSecretKey = clientSecretKey;
    }

    public String getAdhocDefaultQueue() {
        return adhocDefaultQueue;
    }

    public void setAdhocDefaultQueue(String adhocDefaultQueue) {
        this.adhocDefaultQueue = adhocDefaultQueue;
    }

    @Override
    public String toString() {
        return "FDPGatewayBoxConfiguration{" +
                "boxType='" + boxType + '\'' +
                ", gringottsUrl='" + gringottsUrl + '\'' +
                ", ironbankUrl='" + ironbankUrl + '\'' +
                ", clientId='" + clientId + '\'' +
                ", clientSecretKey='" + clientSecretKey + '\'' +
                ", adhocDefaultQueue='" + adhocDefaultQueue + '\'' +
                '}';
    }
}
