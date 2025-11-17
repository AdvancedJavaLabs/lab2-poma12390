package itmo.maga.javaparallel.lab2.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import itmo.maga.javaparallel.lab2.common.TaskMessage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class ProducerApp {

    private static final String TASK_QUEUE_NAME = "text_tasks";

    private static final String RABBIT_HOST = "localhost";
    private static final int RABBIT_PORT = 5672;
    private static final String RABBIT_USERNAME = "labuser";
    private static final String RABBIT_PASSWORD = "labpassword";

    private static final String DEFAULT_RESOURCE_NAME = "book.txt";

    public static void main(String[] args) {
        try {
            String corpusText;
            String sourceDescription;

            if (args.length >= 1) {
                String inputFilePath = args[0];
                corpusText = readCorpusFromFile(inputFilePath);
                sourceDescription = "file: " + inputFilePath;
            } else {
                corpusText = readCorpusFromResource(DEFAULT_RESOURCE_NAME);
                sourceDescription = "classpath resource: " + DEFAULT_RESOURCE_NAME;
            }

            List<String> sections = splitIntoParagraphSections(corpusText);

            if (sections.isEmpty()) {
                System.err.println("No non-empty sections found in " + sourceDescription);
                System.exit(1);
            }

            String jobId = UUID.randomUUID().toString();
            int totalSections = sections.size();

            System.out.println("Starting job " + jobId + " with " + totalSections +
                    " sections (source: " + sourceDescription + ")");

            sendTasksToRabbit(jobId, sections, totalSections);

            System.out.println("Job " + jobId + " completed. All sections sent to queue '" + TASK_QUEUE_NAME + "'.");
        } catch (IOException e) {
            System.err.println("Failed to read corpus");
            e.printStackTrace(System.err);
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Unexpected error in ProducerApp");
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    /**
     * Читает весь текстовый корпус из файла в строку UTF-8.
     */
    private static String readCorpusFromFile(String inputFilePath) throws IOException {
        Path path = Paths.get(inputFilePath);
        if (!Files.exists(path)) {
            throw new IOException("File does not exist: " + inputFilePath);
        }
        byte[] bytes = Files.readAllBytes(path);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Читает текстовый файл из ресурсов (classpath) в строку UTF-8.
     * Ожидает, что файл лежит в src/main/resources и попадает в classpath как есть.
     */
    private static String readCorpusFromResource(String resourceName) throws IOException {
        ClassLoader classLoader = ProducerApp.class.getClassLoader();
        try (InputStream in = classLoader.getResourceAsStream(resourceName)) {
            if (in == null) {
                throw new IOException("Resource not found on classpath: " + resourceName);
            }
            byte[] bytes = in.readAllBytes();
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    /**
     * Разбивает текст на секции по параграфам.
     * Параграфом считаем блок текста, отделённый хотя бы одной пустой строкой.
     */
    private static List<String> splitIntoParagraphSections(String text) {
        String normalized = text.replace("\r\n", "\n").replace('\r', '\n');

        String[] rawBlocks = normalized.split("\\n\\s*\\n");

        List<String> sections = new ArrayList<>();
        for (String block : rawBlocks) {
            String trimmed = block.trim();
            if (!trimmed.isEmpty()) {
                sections.add(trimmed);
            }
        }

        System.out.println("Detected " + sections.size() + " non-empty paragraph sections");
        return sections;
    }

    /**
     * Отправляет каждую секцию как отдельное сообщение в очередь RabbitMQ.
     */
    private static void sendTasksToRabbit(String jobId, List<String> sections, int totalSections)
            throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT_HOST);
        factory.setPort(RABBIT_PORT);
        factory.setUsername(RABBIT_USERNAME);
        factory.setPassword(RABBIT_PASSWORD);

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.findAndRegisterModules();

            AMQP.BasicProperties messageProperties = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2)
                    .build();

            for (int index = 0; index < sections.size(); index++) {
                String sectionText = sections.get(index);

                TaskMessage taskMessage = new TaskMessage(
                        jobId,
                        index,
                        totalSections,
                        sectionText
                );

                byte[] body = objectMapper.writeValueAsBytes(taskMessage);

                channel.basicPublish(
                        "",
                        TASK_QUEUE_NAME,
                        messageProperties,
                        body
                );

                System.out.println(
                        "Sent section " + index +
                                " of " + totalSections +
                                ", text length = " + sectionText.length()
                );
            }
        }
    }
}
