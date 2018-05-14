package com.company;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.imageio.ImageIO;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.util.IOUtils;

/**
 * This class has deliberately designed to contain the following anti-pattern for testing purposes.
 *  - Logger is not static, hence, it is created everytime we create a new class.
 *  - S3Client is not shared across the instance.
 */
public class ImageProcessor {

    private static final String bucket = "dummy-application";
    private static final String bwFolder = "bw-images/";
    private static final String uprightFolder = "upright-images/";
    private static final String greyFolder = "grey-images/";
    private static final String darkenFolder = "darken-images/";
    private static final String brightenFolder = "brighten-images/";

    private static String sqsQueueURL = "";

    private static final Logger logger = LoggerTool.setupLogger("ImageProcessor");
    private final ImageEditor ie = new ImageEditor();
    private final AmazonSQS amazonSQS = AmazonSQSClientBuilder.defaultClient();

    ImageProcessor(String sqsQueueURL) {
        this.sqsQueueURL = sqsQueueURL;
    }

    void parallelStart(int threads) {
        ExecutorService executorService = Executors.newFixedThreadPool(threads, r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        });
        for (int i=0; i < threads; i++) {
            executorService.submit(this::start);
        }
        try {
            executorService.awaitTermination(100L, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            executorService.shutdown();
            e.printStackTrace();
        }
    }

    void start() {
        try {
            while (true) {
                List<Message> messages = amazonSQS
                    .receiveMessage(new ReceiveMessageRequest().withMessageAttributeNames("key").withQueueUrl(sqsQueueURL))
                    .getMessages();
                AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();

                for (Message message: messages) {

                    GreyImageProcessor gip = new GreyImageProcessor();
                    BWImageProcessor bwip = new BWImageProcessor();
                    UprightImageProcessor uip = new UprightImageProcessor();
                    BrightenImageProcessor bip = new BrightenImageProcessor();
                    DarkenImageProcessor dip = new DarkenImageProcessor();

                    String imageKey = message.getMessageAttributes().get("key").getStringValue();
                    String imageName = getNameFromKey(imageKey);
                    String outputFilePath = "/tmp/" + Instant.now().toString() + imageName;
                    try {
                        IOUtils.copy(amazonS3.getObject(bucket, imageKey).getObjectContent(),
                            new FileOutputStream(outputFilePath, false));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    bwip.uploadBWImage(outputFilePath, imageName);
                    TimeUnit.SECONDS.sleep(1);
                    uip.uploadUprightImage(outputFilePath, imageName);
                    TimeUnit.SECONDS.sleep(1);
                    gip.uploadGreyImage(outputFilePath, imageName);
                    TimeUnit.SECONDS.sleep(1);
                    bip.uploadBrightenImage(outputFilePath, imageName);
                    TimeUnit.SECONDS.sleep(1);
                    dip.uploadDarkenImage(outputFilePath, imageName);
                    TimeUnit.SECONDS.sleep(1);

                    deleteFile(outputFilePath);

                    amazonSQS.deleteMessage(sqsQueueURL, message.getReceiptHandle());
                }
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            logger.warning("Image Processor failed: " + e);
            e.printStackTrace();
        }
    }

    private String getNameFromKey(String key) {
        String[] keySplit = key.split("/");
        return keySplit[keySplit.length-1];
    }

    private class GreyImageProcessor {
        private final Logger logger = LoggerTool.setupLogger("GreyImageProcessor");
        private final AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();

        private void uploadGreyImage(String filePath, String uploadFileName) {
            try {
                String greyFilePath = "/tmp/grey-" + Instant.now().toString() + uploadFileName;
                BufferedImage dest = ie.grey(ImageIO.read(new File(filePath)));
                ImageIO.write(dest,"PNG", new File(greyFilePath));
                PutObjectResult res = amazonS3.putObject(new PutObjectRequest(bucket,
                    greyFolder + uploadFileName + Instant.now().toString(),
                    new File(greyFilePath)));

                res.getContentMd5();
                logger.info("Uploaded grey image successfully.");
                deleteFile(greyFilePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class UprightImageProcessor {

        private final Logger logger = LoggerTool.setupLogger("UprightImageProcessor");
        private final AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();

        private void uploadUprightImage(String filePath, String uploadFileName) {
            try {
                String uprightFilePath = "/tmp/upright-" + Instant.now().toString() + uploadFileName;
                BufferedImage dest = ie.rotateRight180(ImageIO.read(new File(filePath)));
                ImageIO.write(dest,"PNG", new File(uprightFilePath));
                PutObjectResult res = amazonS3.putObject(new PutObjectRequest(bucket,
                    uprightFolder + uploadFileName + Instant.now().toString(),
                    new File(uprightFilePath)));

                res.getContentMd5();
                logger.info("Uploaded upright image successfully.");
                deleteFile(uprightFilePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class BWImageProcessor {

        private final Logger logger = LoggerTool.setupLogger("BWImageProcessor");
        private final AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();

        private void uploadBWImage(String filePath, String uploadFileName) {
            try {
                String bwFilePath = "/tmp/bw-" + Instant.now().toString() + uploadFileName;
                BufferedImage dest = ie.monochrome(ImageIO.read(new File(filePath)));
                ImageIO.write(dest,"PNG", new File(bwFilePath));
                PutObjectResult res = amazonS3.putObject(new PutObjectRequest(bucket,
                    bwFolder + uploadFileName + Instant.now().toString(),
                    new File(bwFilePath)));

                res.getContentMd5();
                logger.info("Uploaded black and white image successfully.");
                deleteFile(bwFilePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class BrightenImageProcessor {

        private final Logger logger = LoggerTool.setupLogger("BrightenImageProcessor");
        private final AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();

        private void uploadBrightenImage(String filePath, String uploadFileName) {
            try {
                String brightFilePath = "/tmp/bright-" + Instant.now().toString() + uploadFileName;
                BufferedImage dest = ie.brightenImage(ImageIO.read(new File(filePath)));
                ImageIO.write(dest,"PNG", new File(brightFilePath));
                PutObjectResult res = amazonS3.putObject(new PutObjectRequest(bucket,
                    brightenFolder + uploadFileName + Instant.now().toString(),
                    new File(brightFilePath)));

                res.getContentMd5();
                logger.info("Uploaded brighten image successfully.");
                deleteFile(brightFilePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class DarkenImageProcessor {

        private final Logger logger = LoggerTool.setupLogger("DarkenImageProcessor");
        private final AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();

        private void uploadDarkenImage(String filePath, String uploadFileName) {
            try {
                String darkFilePath = "/tmp/dark-" + Instant.now().toString() + uploadFileName;
                BufferedImage dest = ie.darkenImage(ImageIO.read(new File(filePath)));
                ImageIO.write(dest,"PNG", new File(darkFilePath));
                PutObjectResult res = amazonS3.putObject(new PutObjectRequest(bucket,
                    darkenFolder + uploadFileName + Instant.now().toString(),
                    new File(darkFilePath)));

                res.getContentMd5();
                logger.info("Uploaded darken image successfully.");
                deleteFile(darkFilePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void deleteFile(String filePath) {
        if(!new File(filePath).delete()) {
            logger.warning("Fail to remove file in : " + filePath);
        }
    }
}
