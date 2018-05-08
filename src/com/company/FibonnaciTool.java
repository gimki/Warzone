package com.company;

import java.math.BigInteger;
import java.sql.Time;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;

public class FibonnaciTool {

    private static final Logger logger = LoggerTool.setupLogger("FibonnaciTool");
    public static final String FIBONNACI_TABLE = "fibonnaci-table";

    public static BigInteger fib(BigInteger n) {
        if (n.compareTo(BigInteger.ONE) == -1 || n.compareTo(BigInteger.ONE) == 0 ) return n;
        else
            return fib(n.subtract(BigInteger.ONE)).add(fib(n.subtract(BigInteger.ONE).subtract(BigInteger.ONE)));
    }

    static void populateFibInParallelUpTo(BigInteger max, int numOfThread) {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(FIBONNACI_TABLE);
        BigInteger startFrom = pickUpFrom(table);
        ExecutorService executorService = Executors.newFixedThreadPool(numOfThread);
        for (int j = 0; j < numOfThread; j++) {
            final int ID = j;
            executorService.submit(() -> {
                try {
                    BigInteger order = startFrom;
                    while (true) {
                        table.putItem(new Item().withNumber("workerId", ID)
                            .withString("order", order.toString())
                            .withString("fibonnaci", fib(order).toString()));
                        order = order.add(BigInteger.ONE);
                        try {
                            TimeUnit.MILLISECONDS.sleep(100);
                        } catch (InterruptedException e) {
                            logger.warning("Interrupted during prime calculation.");
                        }
                        if (order.compareTo(max)==1) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    logger.warning("Fibonacci Tool failed: " + e);
                }
            });
        }
        try {
            executorService.awaitTermination(100L, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            executorService.shutdown();
            e.printStackTrace();
        }
    }

    private static BigInteger pickUpFrom(Table table) {
        BigInteger largestPrime = BigInteger.ZERO;
        ItemCollection<QueryOutcome> results = table.query(new QuerySpec()
            .withHashKey("workerId", 1));
        for (Item result : results) {
            BigInteger number = new BigInteger(result.getString("order"));
            if (number.compareTo(largestPrime) == 1) {
                largestPrime = number;
            }
        };
        return largestPrime;
    }
}

