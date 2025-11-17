package itmo.maga.javaparallel.lab2.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import itmo.maga.javaparallel.lab2.common.FinalJobResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Result sink / storage.
 *
 * Слушает очередь с финальными результатами (FinalJobResult)
 * и сохраняет каждый результат в JSON-файл для отчёта.
 */
public final class ResultSinkApp {

    private static final String FINAL_RESULT_QUEUE_NAME = "text_final_results";

    private static final String RABBIT_HOST = "localhost";
    private static final int RABBIT_PORT = 5672;
    private static final String RABBIT_USERNAME = "labuser";
    private static final String RABBIT_PASSWORD = "labpassword";

    private static final Path OUTPUT_DIR = Paths.get("results");

    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    public static void main(String[] args) {
        try {
            runSink();
        } catch (Exception e) {
            System.err.println("Result sink failed with unexpected error");
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        return mapper;
    }

    private static void runSink() throws IOException, TimeoutException {
        // Гарантируем, что каталог существует
        Files.createDirectories(OUTPUT_DIR);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT_HOST);
        factory.setPort(RABBIT_PORT);
        factory.setUsername(RABBIT_USERNAME);
        factory.setPassword(RABBIT_PASSWORD);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(FINAL_RESULT_QUEUE_NAME, true, false, false, null);

        channel.basicQos(1);

        String sinkId = buildSinkId();
        System.out.println(
                "Result sink " + sinkId +
                        " started. Waiting for messages from '" + FINAL_RESULT_QUEUE_NAME + "'..."
        );

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            long deliveryTag = delivery.getEnvelope().getDeliveryTag();
            try {
                byte[] body = delivery.getBody();
                FinalJobResult result = OBJECT_MAPPER.readValue(body, FinalJobResult.class);

                if (result == null) {
                    System.err.println("Result sink: received null FinalJobResult, skipping");
                    channel.basicAck(deliveryTag, false);
                    return;
                }

                Path outputPath = buildOutputPath(result);
                writeResultToFile(result, outputPath);

                System.out.println(
                        "Result sink " + sinkId +
                                " saved result for job " + result.getJobId() +
                                " to " + outputPath.toAbsolutePath()
                );

                channel.basicAck(deliveryTag, false);
            } catch (Exception ex) {
                System.err.println(
                        "Result sink " + sinkId +
                                " failed to process message, will requeue"
                );
                ex.printStackTrace(System.err);
                channel.basicNack(deliveryTag, false, true);
            }
        };

        boolean autoAck = false;
        channel.basicConsume(
                FINAL_RESULT_QUEUE_NAME,
                autoAck,
                deliverCallback,
                consumerTag -> System.out.println(
                        "Result sink " + sinkId + " cancelled consumer: " + consumerTag
                )
        );
    }

    private static Path buildOutputPath(FinalJobResult result) {
        String jobId = result.getJobId() != null ? result.getJobId() : "unknown";
        String fileName = "job-" + jobId + ".json";
        return OUTPUT_DIR.resolve(fileName);
    }

    private static void writeResultToFile(FinalJobResult result, Path outputPath) throws IOException {
        OBJECT_MAPPER.writeValue(outputPath.toFile(), result);
    }

    private static String buildSinkId() {
        String threadPart = Thread.currentThread().getName();
        String randomPart = UUID.randomUUID().toString().substring(0, 8);
        return threadPart + "-" + randomPart;
    }
}
