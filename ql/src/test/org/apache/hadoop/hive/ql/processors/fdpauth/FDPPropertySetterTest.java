package org.apache.hadoop.hive.ql.processors.fdpauth;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;

/**
 * Created by kartik.bhatia on 12/03/18.
 */
public class FDPPropertySetterTest {
    private FDPAuth fdpAuth;

    @Before
    public void setUp() throws Exception {
        File file = new File(FDPAuth.BUCKET_FILE);
        file.createNewFile();
        FileWriter writer = new FileWriter(file);
        writer.write("\"/Users/kartik.bhatia/work/hive/ql/src/test/org/apache/hadoop/hive/ql/processors/fdpauth/local-prop.json\"");
        writer.close();
        fdpAuth = FDPAuth.getInstance();
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