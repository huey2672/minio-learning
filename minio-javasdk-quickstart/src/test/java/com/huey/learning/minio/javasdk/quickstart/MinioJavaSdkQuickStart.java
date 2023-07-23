package com.huey.learning.minio.javasdk.quickstart;

import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.ObjectWriteArgs;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class MinioJavaSdkQuickStart {

    private MinioClient minioClient;

    @Before
    public void initMinioClient() {
        minioClient = MinioClient.builder()
                .endpoint("http://127.0.0.1:9000")
                .credentials("admin", "password")
                .build();
    }

    @Test
    public void testMakeBucket() throws Exception {
        MakeBucketArgs makeBucketArgs = MakeBucketArgs.builder()
                .bucket("mybucket1")
                .build();
        minioClient.makeBucket(makeBucketArgs);
    }

    @Test
    public void testBucketExists() throws Exception {
        BucketExistsArgs bucketExistsArgs = BucketExistsArgs.builder()
                .bucket("mybucket1")
                .build();
        boolean bucketExists = minioClient.bucketExists(bucketExistsArgs);
        System.out.println("bucketExists = " + bucketExists);
    }

    @Test
    public void testPutObject() throws Exception {
        PutObjectArgs putObjectArgs = PutObjectArgs.builder()
                .bucket("mybucket1")
                .object("passwd")
                .stream(new FileInputStream("/etc/passwd"), -1, ObjectWriteArgs.MIN_MULTIPART_SIZE)
                .build();
        ObjectWriteResponse objectWriteResponse = minioClient.putObject(putObjectArgs);
    }

    @Test
    public void testGetObject() throws Exception {
        GetObjectArgs getObjectArgs = GetObjectArgs.builder()
                .bucket("mybucket1")
                .object("passwd")
                .build();
        try (InputStream stream = minioClient.getObject(getObjectArgs)) {
            // Read the stream
            String content = IOUtils.toString(stream, StandardCharsets.UTF_8);
            System.out.println(content);
        }
    }

}
