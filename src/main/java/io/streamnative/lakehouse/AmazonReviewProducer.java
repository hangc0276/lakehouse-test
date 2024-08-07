package io.streamnative.lakehouse;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class AmazonReviewProducer {

    @Parameters(commandDescription = "Test pulsar producer performance.")
    static class Arguments {
        @Parameter(names = { "-r", "--rate" }, description = "Publish rate msg/s across topics")
        public int msgRate = 1000;
        @Parameter(names = {"--send-timeout"}, description = "Set the sendTimeout value default 0 to keep "
                + "compatibility with previous version of pulsar-perf")
        public int sendTimeout = 0;
        @Parameter(names = { "-z", "--compression" }, description = "Compress messages payload")
        public CompressionType compression = CompressionType.NONE;

        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;
        @Parameter(names = { "-u", "--service-url" }, description = "Pulsar Service URL", required = true)
        public String serviceURL;

        @Parameter(description = "persistent://prop/ns/my-topic", required = true)
        public String topic;

        @Parameter(names = {"-f", "--file"}, description = "File path to read Amazon reviews from", required = true)
        public String filePath;

        @Parameter(names = { "--token" }, description = "Authentication token of the API key")
        public String token;
    }

    public static void main(String[] args) throws Exception {
        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("Amazon review producer");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            jc.usage();
            System.exit(1);
        }

        if (arguments.help) {
            jc.usage();
            return;
        }

        RateLimiter rateLimiter = RateLimiter.create(arguments.msgRate);
        ClientBuilder builder = PulsarClient.builder().serviceUrl(arguments.serviceURL);

        if (!StringUtils.isBlank(arguments.token)) {
            builder.authentication(AuthenticationFactory.token(arguments.token));
        }

        PulsarClient client = builder.build();
        // Create a producer on the topic.
        Producer<AmazonReview> producer = client.newProducer(Schema.AVRO(AmazonReview.class))
                .topic(arguments.topic)
                .compressionType(arguments.compression)
                .sendTimeout(arguments.sendTimeout, TimeUnit.SECONDS)
                .blockIfQueueFull(true)
                .enableBatching(true)
                .create();

        if (!Files.exists(Path.of(arguments.filePath))) {
            System.err.println("File not found: " + arguments.filePath);
            System.exit(1);
        }

        AtomicLong totalSent = new AtomicLong(0);
        AtomicLong failedSent = new AtomicLong(0);

        int totalLines = countTotalLines(arguments.filePath);
        CountDownLatch latch = new CountDownLatch(totalLines);
        log.info("Start read file: {} with total lines: {}", arguments.filePath, totalLines);

        try (BufferedReader br = new BufferedReader(new FileReader(arguments.filePath))) {
            String line;

            int count = 0;
            while ((line = br.readLine()) != null) {
                count++;
                if (count % 1000 == 0) {
                    log.info("Sending {} lines", count);
                }
                // Parse the line
                String[] fields = parseLine(line);

                // Assuming the schema is polarity, title, text
                String polarity = fields[0];
                String title = fields[1];
                String text = fields[2];

                AmazonReview review = new AmazonReview(polarity, title, text);
                producer.newMessage()
                        .value(review)
                        .sendAsync()
                        .whenComplete((msgId, throwable) -> {
                            if (throwable != null) {
                                failedSent.incrementAndGet();
                            } else {
                                totalSent.incrementAndGet();
                            }
                            latch.countDown();
                        });
            }

            latch.await();
            log.info("Total sent: {}", totalSent.get());
            log.info("Failed sent: {}", failedSent.get());

            producer.close();
            client.close();
        } catch (IOException e) {
            log.error("Failed to send messages ", e);
        }
    }

    private static String[] parseLine(String line) {
        // Use regex to split by comma, but ignore commas inside quotes
        String regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
        String[] fields = line.split(regex);

        // Remove quotes from the fields
        for (int i = 0; i < fields.length; i++) {
            fields[i] = fields[i].replaceAll("^\"|\"$", "");
        }

        return fields;
    }

    private static int countTotalLines(String filePath) {
        int lineCount = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            while (br.readLine() != null) {
                lineCount++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lineCount;
    }
}
