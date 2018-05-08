package com.company;

import java.math.BigInteger;
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

public class PrimeTool {

    private static final Logger logger = LoggerTool.setupLogger("PrimeTool");
    public static final String PRIME_TABLE = "prime-table";

    /*
    from startFrom exclusively.
     */
    private static BigInteger findNextPrime(BigInteger startFrom) {
        BigInteger number = startFrom;
        while(true) {
            number = number.add(BigInteger.ONE);
            if(isPrime(number)) {
                return number;
            }
        }
    }

    private static boolean isPrime(BigInteger number) {
        try {
            if (!number.isProbablePrime(1024))
                return false;

            //check if even
            BigInteger two = new BigInteger("2");
            if (!two.equals(number) && BigInteger.ZERO.equals(number.mod(two)))
                return false;

            //find divisor if any from 3 to 'number'
            for (BigInteger i = new BigInteger("3"); i.multiply(i).compareTo(number) < 1; i = i.add(two)) {
                //start from 3, 5, etc. the odd number, and look for a divisor if any
                if (BigInteger.ZERO.equals(number.mod(i))) //check if 'i' is divisor of 'number'
                    return false;
            }
            return true;
        } catch (Exception e) {
            logger.warning("isPrime check failed " + e);
            return false;
        }
    }

    static void populatePrimeUpTo(BigInteger max) {
        try {
            AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
            DynamoDB dynamoDB = new DynamoDB(client);
            Table table = dynamoDB.getTable(PRIME_TABLE);

            BigInteger previousPrime = pickUpFrom(table);
            while (true) {
                BigInteger nextPrime = findNextPrime(previousPrime);
                table.putItem(new Item().withString("key-not-used","1")
                    .withString("prime", nextPrime.toString()));
                previousPrime = nextPrime;
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                    logger.warning("Interrupted during prime calculation.");
                }
                if (nextPrime.compareTo(max)==1) {
                    break;
                }
            }
        } catch (Exception e) {
            logger.warning("PrimeTool failed due to :" + e);
        }
    }

    private static BigInteger pickUpFrom(Table table) {
        BigInteger largestPrime = BigInteger.ONE;
        ItemCollection<QueryOutcome> results = table.query(new QuerySpec()
            .withHashKey("key-not-used", "1"));
        for (Item result : results) {
            BigInteger number = new BigInteger(result.getString("prime"));
            if (number.compareTo(largestPrime) == 1) {
                largestPrime = number;
            }
        };
        return largestPrime;
    }
}
