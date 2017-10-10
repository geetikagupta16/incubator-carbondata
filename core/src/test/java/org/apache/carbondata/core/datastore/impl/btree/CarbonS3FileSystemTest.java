package org.apache.carbondata.core.datastore.impl.btree;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.datastore.impl.CarbonS3FileSystem;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;

public class CarbonS3FileSystemTest {

    CarbonS3FileSystem carbonS3FileSystem = new CarbonS3FileSystem();

    @Test
    public void testDeleteSuccessCase() {
        //pending
        Path path = new Path("/abc");
        try {
            carbonS3FileSystem.delete(path, false);
        } catch (IOException e) {
            Assert.assertTrue(false);
            e.printStackTrace();
        }
    }

    // Done
    @Test
    public void testGetFileStatusForEmptyPath() {
        Path path = new Path("/path");

        new MockUp<Path>() {
            @Mock
            public String getName() {
                return "";
            }

            @Mock
            public Path makeQualified(FileSystem fs) {
                return path;
            }
        };

        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                return new ObjectMetadata();
            }
        };

        writeFields();
        try {
            FileStatus fileStatus = carbonS3FileSystem.getFileStatus(path);
            Assert.assertEquals(0 ,fileStatus.getLen());
            Assert.assertEquals(path, fileStatus.getPath());
        } catch (IOException e) {
            Assert.assertTrue(false);
        }
    }

    // Done
    @Test
    public void testGetFileStatusForEmptyPathAndNullMetadata() {
        new MockUp<Path>() {
            @Mock
            public String getName() {
                return "";
            }

            @Mock
            public Path makeQualified(URI defaultUri, Path workingDir) {
                return new Path("/path");
            }
        };

        new MockUp<AmazonS3Client>() {
            @Mock
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                System.out.println("--------------mocking here");
                return null;
            }
        };

        new MockUp<URI>() {
            @Mock
            public String getHost() {
                return "host";
            }
        };

        writeFields();
        Path path = new Path("/path");
        try {
            carbonS3FileSystem.getFileStatus(path);
        } catch (FileNotFoundException fileNotFoundException) {
            Assert.assertTrue(true);
        } catch (IOException e) {
            Assert.assertTrue(false);
        }
    }

    private void writeFields() {
        try {
            Field s3field = carbonS3FileSystem.getClass().getDeclaredField("s3");
            Field urifield = carbonS3FileSystem.getClass().getDeclaredField("uri");

            s3field.setAccessible(true);
            urifield.setAccessible(true);
            FieldUtils.writeDeclaredField(carbonS3FileSystem, "s3", new AmazonS3Client(), true);
            FieldUtils.writeDeclaredField(carbonS3FileSystem, "uri", new URI("uri"), true);
        } catch (NoSuchFieldException | IllegalAccessException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    //Done
    @Test
    public void testGetFileStatusForIllegalArgumentException() {
        new MockUp<Path>() {
            @Mock
            public String getName() {
                return "";
            }

            @Mock
            public Path makeQualified(URI defaultUri, Path workingDir) {
                return new Path("/path");
            }
        };

        new MockUp<AmazonS3Client>() {
            public ObjectMetadata getObjectMetadata(String bucketName, String key) {
                return null;
            }
        };

        new MockUp<URI>() {
            public String getHost() {
                return "host";
            }
        };

        writeFields();
        Path path = new Path("/path");
        try {
            carbonS3FileSystem.getFileStatus(path);
        } catch (IllegalArgumentException illegalArgumentException) {
            Assert.assertTrue(true);
        } catch (IOException e) {
            Assert.assertTrue(false);
        }
    }

}
