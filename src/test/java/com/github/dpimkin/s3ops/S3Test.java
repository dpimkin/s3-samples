package com.github.dpimkin.s3ops;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;


@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class S3Test {

    static DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:0.11.3");

    @Container
    public static final LocalStackContainer localstack = new LocalStackContainer(localstackImage)
            .withServices(S3);

    private S3AsyncClient s3AsyncClient;
    private S3TransferManager transferManager;


    @BeforeEach
    void initClient() {
        s3AsyncClient = S3AsyncClient.builder()
                .endpointOverride(localstack.getEndpointOverride(S3))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .region(Region.of(localstack.getRegion()))
                .build();

        transferManager = S3TransferManager.builder()
                .s3Client(s3AsyncClient)
                .build();
    }



    @Test
    void test() throws Exception {
        var tmp = tempFile();
        populateFile(tmp, 1005042);

        var res = createBucket("foo").thenApply(r -> uploadFile(tmp, "foo", "bar")).join();
        System.out.println(res.completionFuture().get().response().eTag());


    }


    CompletableFuture<CreateBucketResponse> createBucket(String bucketName) {
        return s3AsyncClient.createBucket(b -> b.bucket(bucketName));
    }


    FileUpload uploadFile(File fileToUpload, String bucket, String key) {
        return transferManager.uploadFile(builder -> builder.putObjectRequest(b -> b.bucket(bucket).key(key))
                .addTransferListener(LoggingTransferListener.create())
                .source(fileToUpload.getAbsoluteFile()));
    }


    File tempFile() throws Exception {
        var tempFile = File.createTempFile("some", ".tmp");
        tempFile.deleteOnExit();


        return tempFile;
    }

    void populateFile(File file, int fileSizeBytes) throws Exception {
        try (var fos = new FileOutputStream(file)) {
            byte[] buffer = new byte[1024];
            Random random = ThreadLocalRandom.current();

            while (file.length() < fileSizeBytes) {
                random.nextBytes(buffer);
                fos.write(buffer);
            }
        }
    }

}
