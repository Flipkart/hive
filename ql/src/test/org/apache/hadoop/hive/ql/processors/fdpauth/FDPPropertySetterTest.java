package org.apache.hadoop.hive.ql.processors.fdpauth;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by kartik.bhatia on 12/03/18.
 */
public class FDPPropertySetterTest {
    private FDPAuth fdpAuth;
    private static final String BUCKET_FILE = "/Users/kartik.bhatia/work/hive/ql/src/test/org/apache/hadoop/hive/ql/processors/fdpauth/bucketfile";
    @Before
    public void setUp() throws Exception {
        fdpAuth = FDPAuth.getInstance(BUCKET_FILE);
    }

    @Test
    public void testIsUserSpecificPropertiesToBeSetForWhiteListedIp() throws Exception {
        String requestingIp = "127.0.0.1";
        String actualWhiteListedIp = "127.0.0.1";
        fdpAuth.getConfig().setWhiteListedIps(actualWhiteListedIp);
        fdpAuth.setCurrentIp(requestingIp);
        org.junit.Assert.assertEquals(
                "User specific property being set for whitelisted ip",
                false, FDPPropertySetter.isUserSpecificPropertiesToBeSetForRequestigIp());
    }

    @Test
    public void testIsUserSpecificPropertiesToBeSetForNormalIp() throws Exception {
        String requestingIp = "127.0.0.1";
        fdpAuth.getSetOfWhiteListedIps().remove(requestingIp);
        fdpAuth.setCurrentIp(requestingIp);
        Assert.assertEquals("User property not being set for non-whitelisted ip", true, FDPPropertySetter.isUserSpecificPropertiesToBeSetForRequestigIp());
    }
}