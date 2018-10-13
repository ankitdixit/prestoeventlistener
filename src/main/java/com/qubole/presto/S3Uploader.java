package com.qubole.presto;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.commons.io.FileUtils.writeStringToFile;

/**
 * Created by ankit on 10/9/16.
 */
public class S3Uploader implements Callable{
    private static final char COLUMN_DELIMITER = '|';
    private static final char MAP_KEY_VALUE_DELIM = 3;
    private static final char MAP_ENTRY_DELIM = 2;
    private static final String FILE_FORMAT = ".txt";

    private BlockingQueue<String> queue;

    private AmazonS3 s3Client;

    private String s3Bucket;
    private String s3TableLocationKey;

    private volatile boolean running = true;


    public S3Uploader(String awsAccessKey, String awsSecretKey, String s3Bucket, String s3TableLocationKey) {
        this.s3Bucket = s3Bucket;
        this.s3TableLocationKey = s3TableLocationKey;

        queue = new LinkedBlockingQueue();
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withRegion(Regions.US_EAST_1)
                .build();
    }

    public void queueUpload(String peakMemStats) {
        queue.add(peakMemStats);
    }

    public Object call() throws Exception {
        while (running)
        {
            PutObjectRequest putObjectRequest = getRequest(queue.take());
            uploadToS3(putObjectRequest);
        }
        return Boolean.TRUE;
    }

    public void uploadToS3(PutObjectRequest request) {
        s3Client.putObject(request);
        try {
            File file = new File("/tmp/uploadLogs");
            writeStringToFile(file, "uploaded to s3");
        } catch (Exception e) {

        }
    }

    public PutObjectRequest getRequest (String row) throws IOException {
        File file = new File("tempfile");
        FileUtils.writeStringToFile(file, row);
        PutObjectRequest request = new PutObjectRequest(s3Bucket, s3TableLocationKey+"/"+ System.currentTimeMillis()+FILE_FORMAT, file);
        return  request;
    }

    public boolean isQueueEmpty() {
        return queue.isEmpty();
    }

}
