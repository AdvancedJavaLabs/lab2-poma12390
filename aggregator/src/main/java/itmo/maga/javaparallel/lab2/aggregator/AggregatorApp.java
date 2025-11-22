package itmo.maga.javaparallel.lab2.aggregator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import itmo.maga.javaparallel.lab2.common.FinalJobResult;
import itmo.maga.javaparallel.lab2.common.ResultMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class AggregatorApp {

    private static final String RESULT_QUEUE_NAME = "text_results";
    private static final String FINAL_RESULT_QUEUE_NAME = "text_final_results";

    private static final String RABBIT_HOST = "localhost";
    private static final int RABBIT_PORT = 5672;
    private static final String RABBIT_USERNAME = "labuser";
    private static final String RABBIT_PASSWORD = "labpassword";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final Map<String, JobAggregation> JOBS = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT_HOST);
        factory.setPort(RABBIT_PORT);
        factory.setUsername(RABBIT_USERNAME);
        factory.setPassword(RABBIT_PASSWORD);

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare(RESULT_QUEUE_NAME, true, false, false, null);
            channel.queueDeclare(FINAL_RESULT_QUEUE_NAME, true, false, false, null);
            channel.basicQos(1);

            String aggregatorId = buildAggregatorId();

            System.out.println(
                    "Aggregator " + aggregatorId +
                            " started. Waiting for messages from '" + RESULT_QUEUE_NAME + "'..."
            );

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                try {
                    byte[] body = delivery.getBody();
                    ResultMessage result = OBJECT_MAPPER.readValue(body, ResultMessage.class);

                    if (result == null) {
                        System.err.println("Aggregator: received null ResultMessage, skipping");
                        channel.basicAck(deliveryTag, false);
                        return;
                    }

                    String jobId = result.getJobId();
                    if (jobId == null || jobId.isEmpty()) {
                        System.err.println("Aggregator: received ResultMessage with empty jobId, skipping");
                        channel.basicAck(deliveryTag, false);
                        return;
                    }

                    int totalSections = result.getTotalSections();
                    if (totalSections <= 0) {
                        System.err.println(
                                "Aggregator: received ResultMessage with non-positive totalSections for job " + jobId
                        );
                        channel.basicAck(deliveryTag, false);
                        return;
                    }

                    JobAggregation job = JOBS.computeIfAbsent(
                            jobId,
                            id -> new JobAggregation(id, totalSections)
                    );

                    job.addSectionResult(result);

                    if (job.isComplete()) {
                        FinalJobResult finalResult = buildFinalResult(job);

                        byte[] finalBody = OBJECT_MAPPER.writeValueAsBytes(finalResult);

                        channel.basicPublish(
                                "",
                                FINAL_RESULT_QUEUE_NAME,
                                null,
                                finalBody
                        );

                        System.out.println(
                                "Aggregator: job " + jobId +
                                        " is complete. Final wordCount = " + finalResult.getTotalWordCount() +
                                        ", sections = " + finalResult.getTotalSections()
                        );

                        JOBS.remove(jobId);
                    }

                    channel.basicAck(deliveryTag, false);
                } catch (Exception ex) {
                    ex.printStackTrace(System.err);
                    channel.basicNack(deliveryTag, false, true);
                }
            };

            boolean autoAck = false;
            channel.basicConsume(
                    RESULT_QUEUE_NAME,
                    autoAck,
                    deliverCallback,
                    consumerTag -> System.out.println(
                            "Aggregator " + aggregatorId + " cancelled consumer: " + consumerTag
                    )
            );
        } catch (IOException | TimeoutException e) {
            System.err.println("Aggregator failed with unexpected error");
            e.printStackTrace(System.err);
        }
    }

    private static FinalJobResult buildFinalResult(JobAggregation job) {
        int totalSections = job.getTotalSections();
        int totalWordCount = job.getTotalWordCount();

        Map<String, Integer> globalFreq = job.getGlobalWordFrequencies();

        List<ResultMessage.WordFrequency> globalTopWords = globalFreq.entrySet()
                .stream()
                .sorted(new Comparator<Map.Entry<String, Integer>>() {
                    @Override
                    public int compare(
                            Map.Entry<String, Integer> o1,
                            Map.Entry<String, Integer> o2
                    ) {
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

        List<ResultMessage> orderedSections = new ArrayList<>(job.getSections().values());
        Collections.sort(orderedSections, new Comparator<ResultMessage>() {
            @Override
            public int compare(ResultMessage o1, ResultMessage o2) {
                return Integer.compare(o1.getSectionIndex(), o2.getSectionIndex());
            }
        });

        String modifiedText = buildModifiedText(orderedSections);
        List<String> sortedSentences = buildSortedSentences(modifiedText);

        double averageSentiment = 0.0;
        if (totalSections > 0) {
            averageSentiment = (double) job.getTotalSentimentScore() / (double) totalSections;
        }

        FinalJobResult finalResult = new FinalJobResult();
        finalResult.setJobId(job.getJobId());
        finalResult.setTotalSections(totalSections);
        finalResult.setTotalWordCount(totalWordCount);
        finalResult.setGlobalTopWords(globalTopWords);
        finalResult.setSections(orderedSections);
        finalResult.setTotalSentimentScore(job.getTotalSentimentScore());
        finalResult.setTotalPositiveWordCount(job.getTotalPositiveWordCount());
        finalResult.setTotalNegativeWordCount(job.getTotalNegativeWordCount());
        finalResult.setAverageSentimentPerSection(averageSentiment);
        finalResult.setModifiedText(modifiedText);
        finalResult.setSortedSentences(sortedSentences);

        return finalResult;
    }

    private static String buildModifiedText(List<ResultMessage> orderedSections) {
        if (orderedSections == null || orderedSections.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < orderedSections.size(); i++) {
            ResultMessage section = orderedSections.get(i);
            String sectionText = section.getTransformedSectionText();
            if (sectionText == null) {
                sectionText = "";
            }
            sb.append(sectionText);
            if (i < orderedSections.size() - 1) {
                sb.append(System.lineSeparator()).append(System.lineSeparator());
            }
        }
        return sb.toString();
    }

    private static List<String> buildSortedSentences(String text) {
        List<String> sentences = new ArrayList<>();
        if (text == null || text.isBlank()) {
            return sentences;
        }

        String normalized = text.replace("\r\n", " ").replace('\n', ' ');
        String[] rawSentences = normalized.split("(?<=[.!?])\\s+");
        for (String raw : rawSentences) {
            if (raw == null) {
                continue;
            }
            String trimmed = raw.trim();
            if (!trimmed.isEmpty()) {
                sentences.add(trimmed);
            }
        }

        sentences.sort(new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                int c = Integer.compare(s1.length(), s2.length());
                if (c != 0) {
                    return c;
                }
                return s1.compareTo(s2);
            }
        });

        return sentences;
    }

    private static String buildAggregatorId() {
        String threadPart = Thread.currentThread().getName();
        String randomPart = UUID.randomUUID().toString().substring(0, 8);
        return threadPart + "-" + randomPart;
    }

    private static final class JobAggregation {

        private final String jobId;
        private final Map<Integer, ResultMessage> sections;
        private final Map<String, Integer> globalWordFrequencies;
        private final Set<Integer> receivedSectionIndexes;

        private int totalSections;
        private int receivedSections;
        private int totalWordCount;

        private int totalSentimentScore;
        private int totalPositiveWordCount;
        private int totalNegativeWordCount;

        JobAggregation(String jobId, int totalSections) {
            this.jobId = jobId;
            this.totalSections = totalSections;
            this.sections = new HashMap<>();
            this.globalWordFrequencies = new HashMap<>();
            this.receivedSectionIndexes = new HashSet<>();
            this.receivedSections = 0;
            this.totalWordCount = 0;
            this.totalSentimentScore = 0;
            this.totalPositiveWordCount = 0;
            this.totalNegativeWordCount = 0;
        }

        String getJobId() {
            return jobId;
        }

        int getTotalSections() {
            return totalSections;
        }

        int getReceivedSections() {
            return receivedSections;
        }

        int getTotalWordCount() {
            return totalWordCount;
        }

        Map<Integer, ResultMessage> getSections() {
            return sections;
        }

        Map<String, Integer> getGlobalWordFrequencies() {
            return globalWordFrequencies;
        }

        int getTotalSentimentScore() {
            return totalSentimentScore;
        }

        int getTotalPositiveWordCount() {
            return totalPositiveWordCount;
        }

        int getTotalNegativeWordCount() {
            return totalNegativeWordCount;
        }

        boolean isComplete() {
            return receivedSections == totalSections;
        }

        void addSectionResult(ResultMessage result) {
            if (result == null) {
                return;
            }

            int sectionIndex = result.getSectionIndex();
            if (receivedSectionIndexes.contains(sectionIndex)) {
                return;
            }

            receivedSectionIndexes.add(sectionIndex);
            receivedSections++;

            sections.put(sectionIndex, result);

            totalWordCount += result.getWordCount();

            totalSentimentScore += result.getSentimentScore();
            totalPositiveWordCount += result.getPositiveWordCount();
            totalNegativeWordCount += result.getNegativeWordCount();

            List<ResultMessage.WordFrequency> topWords = result.getTopWords();
            if (topWords != null) {
                for (ResultMessage.WordFrequency wf : topWords) {
                    if (wf == null) {
                        continue;
                    }
                    String word = wf.getWord();
                    if (word == null || word.isEmpty()) {
                        continue;
                    }
                    int count = wf.getCount();
                    if (count <= 0) {
                        continue;
                    }
                    globalWordFrequencies.merge(word, count, Integer::sum);
                }
            }
        }
    }
}
