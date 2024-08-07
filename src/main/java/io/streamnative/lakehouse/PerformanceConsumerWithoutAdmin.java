package io.streamnative.lakehouse;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.shade.com.google.common.util.concurrent.RateLimiter;

@Slf4j
public class PerformanceConsumerWithoutAdmin {
    private static final String OFFLOAD_CURSOR = "__OFFLOAD";
    private static final int CONSUMER_TIMEOUT = 120_000;


    @Parameters(commandDescription = "Test pulsar offload consumer performance.")
    static class Arguments {
        @Parameter(names = {"-t", "--num-topic"}, description = "Number of topics",
        validateWith = PositiveNumberParameterValidator.class)
        public int numTopics = 1;
        @Parameter(names = {"-time", "--test-duration"},
            description = "Test duration in secs. If <= 0, it will keep consuming")
        public long testTime = 0;
        @Parameter(names = { "-r", "--rate" },
            description = "Consume rate msg/s across topics. If <= 0, it will consume without rate limit.")
        public int msgRate = 0;
        @Parameter(names = {"-ss", "--subscription-name"}, description = "Subscription name, default is test-sub")
        public String subscriptionName = "test-sub";
        @Parameter(names = {"-h", "--help"}, description = "Help message", help = true)
        boolean help;
        @Parameter(names = {"-u", "--service-url"}, description = "Pulsar Service URL")
        public String serviceURL;
        @Parameter(description = "persistent://prop/ns/my-topic", required = true)
        public List<String> topics;
        @Parameter(names = {"--separator"}, description = "Separator between the topic and topic number")
        public String separator = "-";
        @Parameter(names = {"--auth-plugin"}, description = "Consumer timeout in milliseconds")
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
        JCommander jcommander = new JCommander(arguments);
        jcommander.setProgramName("PerformanceConsumer");

        try {
            jcommander.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            jcommander.usage();
            return;
        }

        if (arguments.help) {
            jcommander.usage();
            return;
        }

        for (String arg : arguments.topics) {
            if (arg.startsWith("-")) {
                System.err.printf("invalid option: '%s'\nTo use a topic with the name '%s', "
                    + "please use a fully qualified topic name\n", arg, arg);
                jcommander.usage();
                return;
            }
        }

        if (arguments.topics != null && arguments.topics.size() != arguments.numTopics) {
            // keep compatibility with the previous version
            if (arguments.topics.size() == 1) {
                String prefixTopicName = arguments.topics.get(0);
                List<String> defaultTopics = new ArrayList<>();
                for (int i = 0; i < arguments.numTopics; i++) {
                    defaultTopics.add(prefixTopicName + arguments.separator + i);
                }
                arguments.topics = defaultTopics;
            } else {
                System.err.printf("The number of topics should be equal to the number of topic names\n");
                jcommander.usage();
                return;
            }
        }

        RateLimiter rateLimiter = null;
        if (arguments.msgRate > 0) {
            rateLimiter = RateLimiter.create(arguments.msgRate);
        }
        ClientBuilder builder = PulsarClient.builder().serviceUrl(arguments.serviceURL);

        if (arguments.authPluginClassName != null) {
            builder.authentication(arguments.authPluginClassName, arguments.authParams);
        }

        PulsarClient client = builder.build();
        List<Consumer<Person>> consumers = new ArrayList<>();

        List<String> partitionedTopics = arguments.topics.stream()
            .map(client::getPartitionsForTopic)
            .flatMap(t -> t.join().stream())
            .toList();

        for (String topic : partitionedTopics) {
            Consumer<Person> consumer = client.newConsumer(Schema.AVRO(Person.class))
                .topic(topic)
                .subscriptionType(SubscriptionType.Failover)
                .subscriptionName(arguments.subscriptionName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
            log.info("Subscribed to topic {}", topic);
            consumers.add(consumer);
        }

        Collections.shuffle(consumers);

        log.info("Created {} consumers", consumers.size());
        long startTime = System.nanoTime();
        long testEndTime = startTime + (long) (arguments.testTime * 1e9);
        boolean consumeEnough = false;

        while (true) {
            if (consumeEnough) {
                break;
            }

            log.info("Start new one round of consuming messages ...");
            for (Consumer<Person> consumer : consumers) {
                if (arguments.testTime > 0 && System.nanoTime() > testEndTime) {
                    log.info("------------- DONE (reached the maximum duration: [{} seconds] of consumption) "
                        + "--------------", arguments.testTime);
                    Thread.sleep(5000);
                    consumeEnough = true;
                    log.info("Test time is up, stop consuming.");
                    break;
                }

                consumeOneTopic(consumer, rateLimiter);
            }
        }

        consumers.forEach(c -> {
            try {
                c.close();
            } catch (PulsarClientException e) {
                log.error("Failed to close consumer", e);
                throw new RuntimeException(e);
            }
        });
        client.close();
    }


    public static void consumeOneTopic(Consumer<Person> consumer,
                                RateLimiter rateLimiter) throws Exception {
        String topic = consumer.getTopic();
        log.info("Start to consume messages from topic {} ...", topic);

        while (true) {
            if (rateLimiter != null) {
                rateLimiter.acquire();
            }
            Message<Person> msg =
                consumer.receive(CONSUMER_TIMEOUT, TimeUnit.MILLISECONDS);

            if (msg == null) {
                String errMsg = String.format("Consume receive message timeout. Try next topic. timeout_millis=%s",
                    CONSUMER_TIMEOUT);
                log.error(errMsg);
                break;
            }

            log.info("[{}] Received messageId: {}, data: {}", topic, msg.getMessageId(), msg.getValue());
            consumer.acknowledge(msg);
        }
    }
}


