package org.apache.carbondata.core.locks;


import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.CarbonS3FileSystem;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class S3FileLockTest {

    @Test
    public void lockTest() {

        new MockUp<FileFactory>() {
            @Mock
            public boolean createNewLockFile(String filePath, FileFactory.FileType fileType) {
                return true;
            }

            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType) throws IOException {
                return false;
            }
        };

        new MockUp<CarbonS3FileSystem>() {
            @Mock
            public void initialize(URI uri, Configuration conf) {

            }
        };
        CarbonProperties carbonProperties = CarbonProperties.getInstance();
        carbonProperties.addProperty(CarbonCommonConstants.STORE_LOCATION, "s3a://tmp");
        CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("dbName", "tableName", "tableId");
        S3FileLock s3FileLock = new S3FileLock(carbonTableIdentifier, "lockFile");
        s3FileLock.lock();

    }
}
