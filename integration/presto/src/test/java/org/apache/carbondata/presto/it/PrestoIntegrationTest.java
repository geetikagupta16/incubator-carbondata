package org.apache.carbondata.presto.it;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.presto.PrestoServer;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;


public class PrestoIntegrationTest {

    private static org.apache.carbondata.presto.PrestoServer prestoServer = new PrestoServer();

    private static LogService logger;


    @BeforeClass
    public static void setUp() throws Exception {
        logger = LogServiceFactory.getLogService(PrestoIntegrationTest.class.getCanonicalName());
        String rootPath = new File(PrestoIntegrationTest.class.getResource("/").getPath() + "../../../..").getCanonicalPath();
        String CARBONDATA_STOREPATH = rootPath +"/integration/presto/target/store";
        StoreCreator.createCarbonStore();
        logger.info("\nCarbon Metastore created at location: "+ CARBONDATA_STOREPATH );
        System.out.println("StorePath" + CARBONDATA_STOREPATH);
        prestoServer.prestoJdbcClient(CARBONDATA_STOREPATH );
    }

    @Test
    public void testAggregationQueries(){
        System.out.println(prestoServer.executeQuery("SELECT * FROM testdb.TESTTABLE LIMIT 2"));
    }
}
