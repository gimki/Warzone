package com.company;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Random;
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


public class ImageProcessor {

    private static final Logger logger = LoggerTool.setupLogger("ImageProcessor");
    private static final String bucket = "dummy-application";
    private static final String bwFolder = "bw-images";
    private static final String uprightFolder = "upright-images";
    private static final String greyFolder = "grey-images";
    private static final String darkenFolder = "darken-images";
    private static final String brightenFolder = "brighten-images";

    private static String sqsQueueURL = "";

    private final ImageEditor ie = new ImageEditor();
    private final AmazonSQS amazonSQS = AmazonSQSClientBuilder.defaultClient();
    private final AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();

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
                for (Message message: messages) {
                    String imageKey = message.getMessageAttributes().get("key").getStringValue();
                    String imageName = getNameFromKey(imageKey);
                    String outputFilePath = "/tmp/" + Instant.now().toString() + imageName;
                    try {
                        IOUtils.copy(amazonS3.getObject(bucket, imageKey).getObjectContent(),
                            new FileOutputStream(outputFilePath, false));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    uploadBWImage(outputFilePath, imageName);
                    uploadUprightImage(outputFilePath, imageName);
                    uploadGreyImage(outputFilePath, imageName);
                    uploadBrightenImage(outputFilePath, imageName);
                    uploadDarkenImage(outputFilePath, imageName);

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

    private void uploadGreyImage(String filePath, String uploadFileName) {
        try {
            String greyFilePath = "/tmp/grey-" + Instant.now().toString() + uploadFileName;
            BufferedImage dest = ie.grey(ImageIO.read(new File(filePath)));
            ImageIO.write(dest,"PNG", new File(greyFilePath));
            PutObjectResult res = amazonS3.putObject(new PutObjectRequest(bucket,
                greyFolder + uploadFileName + Instant.now().toString(),
                new File(greyFilePath)));

            res.getContentMd5();
            deleteFile(greyFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uploadUprightImage(String filePath, String uploadFileName) {
        try {
            String uprightFilePath = "/tmp/upright-" + Instant.now().toString() + uploadFileName;
            BufferedImage dest = ie.rotateRight180(ImageIO.read(new File(filePath)));
            ImageIO.write(dest,"PNG", new File(uprightFilePath));
            PutObjectResult res = amazonS3.putObject(new PutObjectRequest(bucket,
                uprightFolder + uploadFileName + Instant.now().toString(),
                new File(uprightFilePath)));
            res.getContentMd5();
            deleteFile(uprightFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uploadBWImage(String filePath, String uploadFileName) {
        try {
            String bwFilePath = "/tmp/bw-" + Instant.now().toString() + uploadFileName;
            BufferedImage dest = ie.monochrome(ImageIO.read(new File(filePath)));
            ImageIO.write(dest,"PNG", new File(bwFilePath));
            PutObjectResult res = amazonS3.putObject(new PutObjectRequest(bucket,
                bwFolder + uploadFileName + Instant.now().toString(),
                new File(bwFilePath)));
            res.getContentMd5();
            deleteFile(bwFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uploadDarkenImage(String filePath, String uploadFileName) {
        try {
            String darkFilePath = "/tmp/dark-" + Instant.now().toString() + uploadFileName;
            BufferedImage dest = ie.darkenImage(ImageIO.read(new File(filePath)));
            ImageIO.write(dest,"PNG", new File(darkFilePath));
            PutObjectResult res = amazonS3.putObject(new PutObjectRequest(bucket,
                darkenFolder + uploadFileName + Instant.now().toString(),
                new File(darkFilePath)));
            res.getContentMd5();
            deleteFile(darkFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uploadBrightenImage(String filePath, String uploadFileName) {
        try {
            String brightFilePath = "/tmp/bright-" + Instant.now().toString() + uploadFileName;
            BufferedImage dest = ie.brightenImage(ImageIO.read(new File(filePath)));
            ImageIO.write(dest,"PNG", new File(brightFilePath));
            PutObjectResult res = amazonS3.putObject(new PutObjectRequest(bucket,
                brightenFolder + uploadFileName + Instant.now().toString(),
                new File(brightFilePath)));
            res.getContentMd5();
            deleteFile(brightFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void deleteFile(String filePath) {
        if(!new File(filePath).delete()) {
            logger.warning("Fail to remove file in : " + filePath);
        }
    }


}
