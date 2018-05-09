package com.company;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClient;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.waiters.Waiter;
import com.amazonaws.waiters.WaiterParameters;

public class Main {

    private static final Logger logger = LoggerTool.setupLogger("Main");
    private static String sqsQueueURL = "";

    public static void main(String[] args) {

        AWSSimpleSystemsManagementClient awsSimpleSystemsManagementClient =
            new AWSSimpleSystemsManagementClient();

        sqsQueueURL = awsSimpleSystemsManagementClient
            .getParameter(new GetParameterRequest().withName("SQSQueueUrl"))
            .getParameter()
            .getValue();

        ExecutorService executorService = Executors.newFixedThreadPool(4);
        ImageProcessor imageProcessor = new ImageProcessor(sqsQueueURL);

        executorService.submit(()-> {
            while(true) {
                PrimeTool.populatePrimeUpTo(new BigInteger("1000000000"));
                logger.info("Populating Prime number finished. Wait for 2 minutes for repopulation.");
                TimeUnit.MINUTES.sleep(2);
                rebuildTable(PrimeTool.PRIME_TABLE);
                TimeUnit.MINUTES.sleep(2);
            }
        });

        executorService.submit(()-> {
            while (true) {
                FibonnaciTool.populateFibInParallelUpTo(new BigInteger("100"), 2);
                logger.info("Populating Fibonnaci number finished. Wait for 2 minutes for repopulation.");
                TimeUnit.MINUTES.sleep(2);
                rebuildTable(FibonnaciTool.FIBONNACI_TABLE);
                TimeUnit.MINUTES.sleep(2);
            }
        });

        executorService.submit(Main::createMessageToImageQueue);

        executorService.submit(()->{
            imageProcessor.parallelStart(3);
        });

        try {
            logger.info("Running for 100 days!");
            executorService.awaitTermination(100, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void rebuildTable(String tableName) {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
        TableDescription tableDescription = client.describeTable(tableName).getTable();
        List<KeySchemaElement> keySchema = tableDescription.getKeySchema();
        List<AttributeDefinition> keyAttributeDefinition = tableDescription.getAttributeDefinitions();
        ProvisionedThroughputDescription provisionedThroughputDescription = tableDescription.getProvisionedThroughput();
        client.deleteTable(tableName);
        Waiter<DescribeTableRequest> waiter = client.waiters().tableNotExists();
        waiter.run(new WaiterParameters<>(new DescribeTableRequest(tableName)));
        client.createTable(new CreateTableRequest(keyAttributeDefinition, tableName, keySchema,
            new ProvisionedThroughput(provisionedThroughputDescription.getReadCapacityUnits(),
                provisionedThroughputDescription.getWriteCapacityUnits())));
    }

    private static List<String> buildListOfItemsInBucket() {
        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();

        List<String> result = new ArrayList();
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName("dummy-application")
            .withPrefix("sample-images/");
        ListObjectsV2Result listing = amazonS3.listObjectsV2(req);
        for (S3ObjectSummary summary: listing.getObjectSummaries()) {
            if (summary.getKey().trim().equals("sample-images/")) {
                continue;
            }
            result.add(summary.getKey().trim());
        }
        return result;
    }

    private static void createMessageToImageQueue() {
        Random randomGenerator = new Random();
        AmazonSQS amazonSQS = AmazonSQSClientBuilder.defaultClient();

        while (true) {
            List<String> list = buildListOfItemsInBucket();
            for (int i = 0; i < 100; i++) {
                String key = list.get(randomGenerator.nextInt(list.size()));

                Map<String, MessageAttributeValue> attributeMap = new HashMap();
                attributeMap.put("key", new MessageAttributeValue()
                    .withDataType("String")
                    .withStringValue(key));

                amazonSQS.sendMessage(new SendMessageRequest()
                    .withMessageAttributes(attributeMap)
                    .withMessageBody("For Image Transformation!")
                    .withQueueUrl(sqsQueueURL));
                try {
                    TimeUnit.MILLISECONDS.sleep(30);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
