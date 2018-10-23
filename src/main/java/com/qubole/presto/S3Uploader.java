package com.qubole.presto;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.io.FileUtils;
import sun.security.jca.GetInstance;

import java.io.*;
import java.lang.reflect.Array;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
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

    private ArrayList<String> stats;

    private Calendar now;

    private AmazonS3 s3Client;

    private String s3Bucket;
    private String s3TableLocationKey;

    private volatile boolean running = true;


    public S3Uploader(String awsAccessKey, String awsSecretKey, String s3Bucket, String s3TableLocationKey) {
        this.s3Bucket = s3Bucket;
        this.s3TableLocationKey = s3TableLocationKey;

        queue = new LinkedBlockingQueue();
        stats = new ArrayList<>();
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withRegion(Regions.US_EAST_1)
                .build();
    }

    public void queueUpload(String peakMemStats) {
        //queue.add(peakMemStats);
        stats.add(peakMemStats);
    }

    public Object call() throws Exception {
        while (running)
        {
            now = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            if(now.get(Calendar.MINUTE) % 15 == 0) {
                //queue.drainTo(stats);
                //queue.clear();
                if(!stats.isEmpty()) {
                    PutObjectRequest putObjectRequest = getRequest(stats);
                    uploadToS3(putObjectRequest);
                    stats.clear();
                    MINUTES.sleep(10);
                }
            }
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

    public PutObjectRequest getRequest (ArrayList<String> rows) throws IOException {
        File file = new File("tempfile");
        FileWriter fw = new FileWriter("tempfile", true);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter out = new PrintWriter(bw);
        for(String row : rows) {
            out.println(row);
        }

        LocalDateTime ldt = LocalDateTime.ofInstant(now.toInstant(), now.getTimeZone().toZoneId());
        DateTimeFormatter format1 = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH);
        String strDate = format1.format(ldt);
        int min = now.get(Calendar.MINUTE);
        int hr;
        int quad;
        if(min < 2)
            hr = now.get(Calendar.HOUR_OF_DAY) - 1;
        else
            hr = now.get(Calendar.HOUR_OF_DAY);
        if(min/15 == 0)
            quad = 4;
        else
            quad = min/15;

        PutObjectRequest request =
                new PutObjectRequest(s3Bucket, s3TableLocationKey + "/" + strDate + "/hour=" + hr + "/quadrant=" + quad + "-" + System.currentTimeMillis()+ FILE_FORMAT, file);
        return  request;
    }

    public boolean isQueueEmpty() {
        return queue.isEmpty();
    }

}
