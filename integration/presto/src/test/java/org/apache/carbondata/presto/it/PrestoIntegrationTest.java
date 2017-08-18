package org.apache.carbondata.presto.it;

import org.junit.BeforeClass;
import org.junit.Test;


public class PrestoIntegrationTest {
    private static String CARBONDATA_STOREPATH = "$rootPath/integration/presto/test/store";
    private static org.apache.carbondata.presto.PrestoServer prestoServer;
    @BeforeClass
    public static void setUp() throws Exception {
        StoreCreator.createCarbonStore();
        prestoServer = new org.apache.carbondata.presto.PrestoServer();
        prestoServer.prestoJdbcClient(CARBONDATA_STOREPATH );
    }
    @Test
    public void demo(){

    }
}
