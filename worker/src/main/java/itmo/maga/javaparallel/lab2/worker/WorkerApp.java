package itmo.maga.javaparallel.lab2.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import itmo.maga.javaparallel.lab2.common.ResultMessage;
import itmo.maga.javaparallel.lab2.common.TaskMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class WorkerApp {

    private static final String TASK_QUEUE_NAME = "text_tasks";
    private static final String RESULT_QUEUE_NAME = "text_results";

    private static final String RABBIT_HOST = "localhost";
    private static final int RABBIT_PORT = 5672;
    private static final String RABBIT_USERNAME = "labuser";
    private static final String RABBIT_PASSWORD = "labpassword";

    public static void main(String[] args) {
        try {
            runWorker();
        } catch (Exception e) {
            System.err.println("Worker failed with unexpected error");
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void runWorker() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT_HOST);
        factory.setPort(RABBIT_PORT);
        factory.setUsername(RABBIT_USERNAME);
        factory.setPassword(RABBIT_PASSWORD);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        channel.queueDeclare(RESULT_QUEUE_NAME, true, false, false, null);

        channel.basicQos(1);

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();

        AMQP.BasicProperties resultProps = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .deliveryMode(2)
                .build();

        String workerId = buildWorkerId();

        System.out.println("Worker " + workerId + " started. Waiting for messages from '" + TASK_QUEUE_NAME + "'...");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            long deliveryTag = delivery.getEnvelope().getDeliveryTag();
            try {
                byte[] body = delivery.getBody();
                TaskMessage task = objectMapper.readValue(body, TaskMessage.class);

                System.out.println(
                        "Worker " + workerId +
                                " received section " + task.getSectionIndex() +
                                " of " + task.getTotalSections() +
                                " for job " + task.getJobId()
                );

                ResultMessage result = processTask(task);

                byte[] resultBody = objectMapper.writeValueAsBytes(result);

                channel.basicPublish(
                        "",
                        RESULT_QUEUE_NAME,
                        resultProps,
                        resultBody
                );

                System.out.println(
                        "Worker " + workerId +
                                " processed section " + task.getSectionIndex() +
                                ", wordCount = " + result.getWordCount()
                );

                channel.basicAck(deliveryTag, false);
            } catch (Exception ex) {
                System.err.println("Worker " + workerId + " failed to process message, will requeue");
                ex.printStackTrace(System.err);
                channel.basicNack(deliveryTag, false, true);
            }
        };

        boolean autoAck = false;
        channel.basicConsume(TASK_QUEUE_NAME, autoAck, deliverCallback, consumerTag -> {
            System.out.println("Worker " + workerId + " cancelled consumer: " + consumerTag);
        });
    }

    private static ResultMessage processTask(TaskMessage task) {
        String text = task.getSectionText();
        List<String> tokens = tokenize(text);

        int wordCount = tokens.size();

        Map<String, Integer> freqMap = new HashMap<>();
        for (String token : tokens) {
            if (!token.isEmpty()) {
                freqMap.merge(token, 1, Integer::sum);
            }
        }

        List<ResultMessage.WordFrequency> topWords = freqMap.entrySet()
                .stream()
                .sorted(new Comparator<Map.Entry<String, Integer>>() {
                    @Override
                    public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                        int c = Integer.compare(o2.getValue(), o1.getValue());
                        if (c != 0) {
                            return c;
                        }
                        return o1.getKey().compareTo(o2.getKey());
                    }
                })
                .limit(10)
                .map(entry -> new ResultMessage.WordFrequency(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        ResultMessage result = new ResultMessage();
        result.setJobId(task.getJobId());
        result.setSectionIndex(task.getSectionIndex());
        result.setTotalSections(task.getTotalSections());
        result.setWordCount(wordCount);
        result.setTopWords(topWords);

        return result;
    }

    private static List<String> tokenize(String text) {
        if (text == null || text.isEmpty()) {
            return new ArrayList<>();
        }

        String normalized = text.toLowerCase();
        String[] parts = normalized.split("[^\\p{L}\\p{Nd}]+");

        List<String> tokens = new ArrayList<>();
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                tokens.add(trimmed);
            }
        }
        return tokens;
    }

    private static String buildWorkerId() {
        String threadPart = Thread.currentThread().getName();
        String randomPart = java.util.UUID.randomUUID().toString().substring(0, 8);
        return threadPart + "-" + randomPart;
    }
}
