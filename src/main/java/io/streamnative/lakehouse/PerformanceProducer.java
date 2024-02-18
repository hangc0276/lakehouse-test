package io.streamnative.lakehouse;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.shade.com.google.common.util.concurrent.RateLimiter;

@Slf4j
public class PerformanceProducer {

    @Parameters(commandDescription = "Test pulsar producer performance.")
    static class Arguments {
        @Parameter(names = { "-t", "--num-topic" }, description = "Number of topics",
            validateWith = PositiveNumberParameterValidator.class)
        public int numTopics = 1;
        @Parameter(names = { "-r", "--rate" }, description = "Publish rate msg/s across topics")
        public int msgRate = 100;
        @Parameter(names = { "-n", "--num-producers" }, description = "Number of producers (per topic)",
            validateWith = PositiveNumberParameterValidator.class)
        public int numProducers = 1;
        @Parameter(names = {"--send-timeout"}, description = "Set the sendTimeout value default 0 to keep "
            + "compatibility with previous version of pulsar-perf")
        public int sendTimeout = 0;
        @Parameter(names = { "-m",
            "--num-messages" }, description = "Number of messages to publish in total. If <= 0, it will keep "
            + "publishing")
        public long numMessages = 0;
        @Parameter(names = { "-z", "--compression" }, description = "Compress messages payload")
        public CompressionType compression = CompressionType.NONE;
        @Parameter(names = { "-time",
            "--test-duration" }, description = "Test duration in secs. If <= 0, it will keep publishing")
        public long testTime = 0;

        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;
        @Parameter(names = { "-u", "--service-url" }, description = "Pulsar Service URL")
        public String serviceURL;

        @Parameter(description = "persistent://prop/ns/my-topic", required = true)
        public List<String> topics;

        @Parameter(names = {"--separator"}, description = "Separator between the topic and topic number")
        public String separator = "-";

        @Parameter(names = { "--auth-plugin" }, description = "Authentication plugin class name")
        public String authPluginClassName;

        @Parameter(
            names = { "--auth-params" },
            description = "Authentication parameters, whose format is determined by the implementation "
                + "of method `configure` in authentication plugin class, for example \"key1:val1,key2:val2\" "
                + "or \"{\"key1\":\"val1\",\"key2\":\"val2\"}\".")
        public String authParams;
    }

    public static void main(String[] args) throws Exception {

        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("pulsar-perf produce");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.out.println(e.getMessage());
            jc.usage();
            return;
        }

        if (arguments.help) {
            jc.usage();
            return;
        }

        for (String arg : arguments.topics) {
            if (arg.startsWith("-")) {
                System.out.printf("invalid option: '%s'\nTo use a topic with the name '%s', "
                    + "please use a fully qualified topic name\n", arg, arg);
                jc.usage();
                return;
            }
        }

        if (arguments.topics != null && arguments.topics.size() != arguments.numTopics) {
            // keep compatibility with the previous version
            if (arguments.topics.size() == 1) {
                String prefixTopicName = arguments.topics.get(0);
                List<String> defaultTopics = new ArrayList<>();
                for (int i = 0; i < arguments.numTopics; i++) {
                    defaultTopics.add(String.format("%s%s%d", prefixTopicName, arguments.separator, i));
                }
                arguments.topics = defaultTopics;
            } else {
                System.out.println("The size of topics list should be equal to --num-topic, topics: "
                    + arguments.topics);
                jc.usage();
                return;
            }
        }

        RateLimiter rateLimiter = RateLimiter.create(arguments.msgRate);
        ClientBuilder builder = PulsarClient.builder()
            .serviceUrl(arguments.serviceURL);

        if (arguments.authPluginClassName != null) {
            builder.authentication(arguments.authPluginClassName, arguments.authParams);
        }

        PulsarClient client = builder.build();

        List<Producer<Person>> producers = new ArrayList<>();
        for (int i = 0; i < arguments.numProducers; ++i) {
            for (String topic : arguments.topics) {
                Producer<Person> producer = client.newProducer(Schema.AVRO(Person.class))
                    .topic(topic)
                    .sendTimeout(arguments.sendTimeout, TimeUnit.MILLISECONDS)
                    .blockIfQueueFull(true)
                    .enableBatching(true)
                    .compressionType(arguments.compression)
                    .create();
                producers.add(producer);
            }
        }

        Collections.shuffle(producers);

        log.info("Created {} producers", producers.size());
        long startTime = System.nanoTime();
        long testEndTime = startTime + (long) (arguments.testTime * 1e9);
        AtomicLong totalSent = new AtomicLong(0);
        boolean produceEnough = false;
        CountDownLatch doneLatch = new CountDownLatch(1);
        long cnt = 0;
        while (true) {
            if (produceEnough) {
                break;
            }

            cnt++;
            if (cnt % 100000 == 0) {
                log.info("sending {}", cnt);
            }

            for (Producer<Person> producer : producers) {
                if (arguments.testTime > 0) {
                    if (System.nanoTime() > testEndTime) {
                        log.info("------------- DONE (reached the maximum duration: [{} seconds] of production) "
                            + "--------------", arguments.testTime);
                        doneLatch.countDown();
                        Thread.sleep(5000);
                        produceEnough = true;
                        break;
                    }
                }

                if (arguments.numMessages > 0) {
                    if (totalSent.get() >= arguments.numMessages) {
                        log.info("------------- DONE (reached the maximum number: {} of production) --------------"
                            , arguments.numMessages);
                        doneLatch.countDown();
                        Thread.sleep(5000);
                        produceEnough = true;
                        break;
                    }
                }

                rateLimiter.acquire();

                TypedMessageBuilder<Person> messageBuilder = producer.newMessage()
                    .value(new Person("hangc", (int) (18 + cnt) % 100,
                        "GuangZhou", true, (59.9 + cnt) % 150, cnt));
                //generate msg key
                messageBuilder.key(String.valueOf(totalSent.get()));
                messageBuilder.sendAsync().thenAccept(msgId -> {
                    totalSent.incrementAndGet();
                });
            }
        }

        producers.forEach(p -> {
            try {
                p.close();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });
        client.close();
    }
}
