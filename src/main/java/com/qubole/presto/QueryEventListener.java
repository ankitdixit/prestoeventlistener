/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qubole.presto;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryInputMetadata;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import io.airlift.units.Duration;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringJoiner;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;

public class QueryEventListener
        implements EventListener
{
    private static final Duration BACKOFF_MIN_SLEEP = new Duration(1, SECONDS);
    private static final Duration maxBackoffTime = new Duration(1, SECONDS);
    private static final Duration maxRetryTime = new Duration(1, SECONDS);
    private static final int maxAttempts = 5;
    private static final String PRESTO_PROTOCOL = "http";
    private static final String PRESTO_HOST = "localhost";
    private static final String PRESTO_PORT = "8081";
    private static final String PRESTO_API = "v1/query/";
    private static final int CONNECTION_TIMEOUT = 500; //in ms
    private static final String POST = "POST";
    private static final String GET = "GET";
    private static final String QUERY = "query";
    private static final String QUERY_INFO_CALLABLE = "getQueryInfoResponse";
    static S3Uploader s3Uploader;
    static DateTimeFormatter formatter =
            DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT)
                    .withLocale(Locale.UK)
                    .withZone(ZoneId.systemDefault());
    final String loggerName = "QueryLog";
    Logger logger;
    FileHandler fh;
    String accessKey;
    String secretKey;
    String bucket;
    String tableLocationKey;

    public QueryEventListener()
    {
        createLogFile();
    }

    public QueryEventListener(Map<String, String> config)
    {
        createLogFile();
        accessKey = config.get("accessKey");
        secretKey = config.get("secretKey");
        bucket = config.get("bucket");
        tableLocationKey = config.get("tableLocationKey");

        s3Uploader = new S3Uploader(accessKey, secretKey, bucket, tableLocationKey);
        FutureTask futureTask = new FutureTask(s3Uploader);
        Thread uploaderThread = new Thread(futureTask);
        uploaderThread.start();
    }

    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        logEvent(queryCreatedEvent);
    }

    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        logEvent(queryCompletedEvent);
        try {

            String prestoQueryId = queryCompletedEvent.getMetadata().getQueryId();

            // we are using the queryInfo json object provided by Presto over the information from queryCompletedEvent as that has more information. We want to send the maximum possible information to Mojave
            String queryInfoURL = PRESTO_PROTOCOL + "://" + PRESTO_HOST + ":" + PRESTO_PORT + "/" + PRESTO_API + prestoQueryId;
            String queryInfoJsonString = getResponseWithRetry(QUERY_INFO_CALLABLE, getCallable(queryInfoURL, GET));

            if (queryInfoJsonString == null) {
                logger.info("Found an error in getting response about queryInfo. Aborting Mojave Plugin for the query presto query id " + prestoQueryId);
                return;
            }

            JSONObject queryInfoJSON = (JSONObject) new JSONParser().parse(queryInfoJsonString);

            String query = ((String) queryInfoJSON.get(QUERY));
            query = query.replaceAll("\n", " ");
            logger.info("Query is :" + query);
            Timestamp createTime = Timestamp.from(queryCompletedEvent.getCreateTime());
            long analysisTime = 0;
            if (queryCompletedEvent.getStatistics().getAnalysisTime().isPresent()) {
                analysisTime = queryCompletedEvent.getStatistics().getAnalysisTime().get().getSeconds();
            }
            Timestamp executionStartTime = Timestamp.from(queryCompletedEvent.getExecutionStartTime());
            Timestamp endTime = Timestamp.from(queryCompletedEvent.getEndTime());
            String state = queryCompletedEvent.getMetadata().getQueryState();
            String error = "NO_ERROR";
            if (queryCompletedEvent.getFailureInfo().isPresent()) {
                error = queryCompletedEvent.getFailureInfo().get().getErrorCode().toString();
            }
            String inputTables = Arrays.toString(queryCompletedEvent.getIoMetadata().getInputs().stream().map(x -> x.getTable()).toArray(String[]::new));
            String inputColumns = Arrays.toString(queryCompletedEvent.getIoMetadata().getInputs().stream().map(x -> getQualifiedColumnNames(x)).toArray(String[]::new));
            long wallTime = queryCompletedEvent.getStatistics().getWallTime().getSeconds();
            long cpuTime = queryCompletedEvent.getStatistics().getCpuTime().getSeconds();
            long totalBytes = queryCompletedEvent.getStatistics().getTotalBytes();
            double totalMemory = queryCompletedEvent.getStatistics().getCumulativeMemory();

            StringJoiner joiner = new StringJoiner("|");
            joiner.add(prestoQueryId)
                    .add(query)
                    .add(createTime.toString())
                    .add(executionStartTime.toString())
                    .add(Long.toString(analysisTime))
                    .add(executionStartTime.toString())
                    .add(endTime.toString())
                    .add(state)
                    .add(error)
                    .add(inputTables)
                    .add(inputColumns)
                    .add(Long.toString(wallTime))
                    .add(Long.toString(cpuTime))
                    .add(Long.toString(totalBytes))
                    .add(Double.toString(totalMemory))
                    .add(queryInfoJsonString);

            s3Uploader.queueUpload(joiner.toString());
        }
        catch (Exception e) {
            System.out.println("Cannot upload into for query");
        }
    }

    private Callable<String> getCallable(String url, String method)
    {
        return getCallable(url, method, "");
    }

    private String getQualifiedColumnNames(QueryInputMetadata queryInputMetadata)
    {
        String tableName = queryInputMetadata.getTable();
        return queryInputMetadata.getColumns().stream().map(x -> tableName + "." + x).collect(Collectors.joining(","));
    }

    private Callable<String> getCallable(String url, String method, String payload)
    {
        return () -> {
            String response = "";
            HttpURLConnection connection = null;
            try {
                URL responseURL = new URL(url);
                connection = getConnection(responseURL, method, payload);
                if (connection != null) {
                    response = getResponse(connection);
                }
            }
            catch (IOException e) {
                throw new Exception(e);
            }
            finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
            return response;
        };
    }

    private String getResponse(HttpURLConnection connection)
            throws IOException
    {
        String inputLine;
        StringBuilder response = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            while ((inputLine = reader.readLine()) != null) {
                response.append(inputLine);
            }
        }
        return response.toString();
    }

    private HttpURLConnection getConnection(URL url, String method, String payload)
            throws IOException
    {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(CONNECTION_TIMEOUT);
        connection.setRequestMethod(method);

        if (method.equalsIgnoreCase(GET)) {
            connection.setRequestProperty("Accept", "application/json");
        }
        else if (method.equalsIgnoreCase(POST)) {
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/octet-stream");
            try (OutputStream output = connection.getOutputStream()) {
                output.write(payload.getBytes());
            }
        }

        //HTTP_CREATED (201) added for Mojave
        if (connection.getResponseCode() == HttpURLConnection.HTTP_OK || connection.getResponseCode() == HttpURLConnection.HTTP_CREATED) {
            return connection;
        }
        else {
            logger.log(Level.SEVERE, String.format("connection returned with error code other than OK. The error code returned is %d, error: %s", connection.getResponseCode(), connection.getResponseMessage()));
            return null;
        }
    }

    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        StringBuilder msg = new StringBuilder();

        try {

            msg.append("---------------Split Completed----------------------------");
            msg.append("\n");
            msg.append("     ");
            msg.append("Query ID: ");
            msg.append(splitCompletedEvent.getQueryId().toString());
            msg.append("\n");
            msg.append("     ");
            msg.append("Stage ID: ");
            msg.append(splitCompletedEvent.getStageId().toString());
            msg.append("\n");
            msg.append("     ");
            msg.append("Task ID: ");
            msg.append(splitCompletedEvent.getTaskId().toString());

            logger.info(msg.toString());
        }
        catch (Exception ex) {
            logger.info(ex.getMessage());
        }
    }

    public void createLogFile()
    {

        SimpleDateFormat dateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        String timeStamp = dateTime.format(new Date());
        StringBuilder logPath = new StringBuilder();

        logPath.append("/tmp/queries-");
        logPath.append(timeStamp);
        logPath.append(".%g.log");

        try {
            logger = Logger.getLogger(loggerName);
            fh = new FileHandler(logPath.toString(), 524288000, 5, true);
            logger.addHandler(fh);
            logger.setUseParentHandlers(false);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);
        }
        catch (IOException e) {
            logger.info(e.getMessage());
        }
    }

    private void logEvent(QueryCreatedEvent queryCreatedEvent)
    {
        StringBuilder msg = new StringBuilder();
        try {
            msg.append("---------------Query Created----------------------------");
            msg.append("\n");
            msg.append("     ");
            msg.append("Query ID: ");
            msg.append(queryCreatedEvent.getMetadata().getQueryId().toString());
            msg.append("\n");
            msg.append("     ");
            msg.append("Query State: ");
            msg.append(queryCreatedEvent.getMetadata().getQueryState().toString());
            msg.append("\n");
            msg.append("     ");
            msg.append("User: ");
            msg.append(queryCreatedEvent.getContext().getUser().toString());
            msg.append("\n");
            msg.append("     ");
            msg.append("Create Time: ");
            msg.append(queryCreatedEvent.getCreateTime());
            msg.append("\n");
            msg.append("     ");
            msg.append("Principal: ");
            msg.append(queryCreatedEvent.getContext().getPrincipal());
            msg.append("\n");
            msg.append("     ");
            msg.append("Remote Client Address: ");
            msg.append(queryCreatedEvent.getContext().getRemoteClientAddress());
            msg.append("\n");
            msg.append("     ");
            msg.append("Source: ");
            msg.append(queryCreatedEvent.getContext().getSource());
            msg.append("\n");
            msg.append("     ");
            msg.append("User Agent: ");
            msg.append(queryCreatedEvent.getContext().getUserAgent());
            msg.append("\n");
            msg.append("     ");
            msg.append("Catalog: ");
            msg.append(queryCreatedEvent.getContext().getCatalog());
            msg.append("\n");
            msg.append("     ");
            msg.append("Schema: ");
            msg.append(queryCreatedEvent.getContext().getSchema());
            msg.append("\n");
            msg.append("     ");
            msg.append("Server Address: ");
            msg.append(queryCreatedEvent.getContext().getServerAddress());

            logger.info(msg.toString());
        }
        catch (Exception ex) {

            logger.info(ex.getMessage());
        }
    }

    private void logEvent(QueryCompletedEvent queryCompletedEvent)
    {
        String errorCode = null;
        StringBuilder msg = new StringBuilder();

        try {
            errorCode = queryCompletedEvent.getFailureInfo().get().getErrorCode().getName().toString();
        }
        catch (NoSuchElementException noElEx) {
            errorCode = null;
        }

        try {

            if (errorCode != null) {

                msg.append("---------------Query Completed----------------------------");
                msg.append("\n");
                msg.append("     ");
                msg.append("Query ID: ");
                msg.append(queryCompletedEvent.getMetadata().getQueryId().toString());
                msg.append("\n");
                msg.append("     ");
                msg.append("Create Time: ");
                msg.append(queryCompletedEvent.getCreateTime());
                msg.append("\n");
                msg.append("     ");
                msg.append("User: ");
                msg.append(queryCompletedEvent.getContext().getUser().toString());
                msg.append("\n");
                msg.append("     ");
                msg.append("Complete: ");
                msg.append(queryCompletedEvent.getStatistics().isComplete());
                msg.append("\n");
                msg.append("     ");
                msg.append("Query Failure Error: ");
                msg.append(errorCode);
                msg.append("\n");
                msg.append("     ");
                msg.append("Remote Client Address: ");
                msg.append(queryCompletedEvent.getContext().getRemoteClientAddress().toString());

                logger.info(msg.toString());
            }
            else {

                msg.append("---------------Query Completed----------------------------");
                msg.append("\n");
                msg.append("     ");
                msg.append("Query ID: ");
                msg.append(queryCompletedEvent.getMetadata().getQueryId().toString());
                msg.append("\n");
                msg.append("     ");
                msg.append("Create Time: ");
                msg.append(queryCompletedEvent.getCreateTime());
                msg.append("\n");
                msg.append("     ");
                msg.append("User: ");
                msg.append(queryCompletedEvent.getContext().getUser().toString());
                msg.append("\n");
                msg.append("     ");
                msg.append("Complete: ");
                msg.append(queryCompletedEvent.getStatistics().isComplete());
                msg.append("\n");
                msg.append("     ");
                msg.append("Remote Client Address: ");
                msg.append(queryCompletedEvent.getContext().getRemoteClientAddress().toString());

                logger.info(msg.toString());
            }
        }
        catch (Exception ex) {
            logger.info(ex.getMessage());
        }
    }

    private String getResponseWithRetry(String callableName, Callable<String> callable)
    {
        String response = null;
        try {
            response = RetryDriver.retry()
                    .maxAttempts(maxAttempts)
                    .exponentialBackoff(BACKOFF_MIN_SLEEP, maxBackoffTime, maxRetryTime, 2.0)
                    .stopOnIllegalExceptions()
                    .stopOn(MalformedURLException.class, IOException.class)
                    .run(callableName, callable);
        }
        catch (Exception e) {
            logger.info("call failed with exception: " + e.toString());
        }
        return response;
    }
}