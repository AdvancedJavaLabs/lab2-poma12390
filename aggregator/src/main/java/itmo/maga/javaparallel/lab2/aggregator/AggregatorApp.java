package itmo.maga.javaparallel.lab2.aggregator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import itmo.maga.javaparallel.lab2.common.ResultMessage;
import itmo.maga.javaparallel.lab2.common.FinalJobResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Aggregator / Collector.
 *
 * Слушает очередь с частичными результатами от воркеров (ResultMessage),
 * агрегирует их по jobId и выводит финальный summary,
 * когда пришли все секции для задания.
 *
 * Сейчас агрегирует:
 *  - суммарный wordCount по всем секциям;
 *  - глобальный топ-N слов (merge top списков секций);
 *  - per-section статистику (для отладки);
 *  - отправляет финальный агрегированный результат (FinalJobResult)
 *    в очередь text_final_results для ResultSinkApp.
 */
public class AggregatorApp {

    private static final String RESULT_QUEUE_NAME = "text_results";
    private static final String FINAL_RESULT_QUEUE_NAME = "text_final_results";

    private static final String RABBIT_HOST = "localhost";
    private static final int RABBIT_PORT = 5672;
    private static final String RABBIT_USERNAME = "labuser";
    private static final String RABBIT_PASSWORD = "labpassword";

    // Сколько слов показывать в итоговом топе
    private static final int GLOBAL_TOP_N = 20;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Состояние активных job-ов по jobId.
     */
    private static final Map<String, JobAggregation> JOBS = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        try {
            runAggregator();
        } catch (Exception e) {
            System.err.println("Aggregator failed with unexpected error");
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void runAggregator() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT_HOST);
        factory.setPort(RABBIT_PORT);
        factory.setUsername(RABBIT_USERNAME);
        factory.setPassword(RABBIT_PASSWORD);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(RESULT_QUEUE_NAME, true, false, false, null);

        channel.queueDeclare(FINAL_RESULT_QUEUE_NAME, true, false, false, null);

        channel.basicQos(1);

        OBJECT_MAPPER.findAndRegisterModules();

        String aggregatorId = buildAggregatorId();
        System.out.println("Aggregator " + aggregatorId +
                " started. Waiting for messages from '" + RESULT_QUEUE_NAME + "'...");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            long deliveryTag = delivery.getEnvelope().getDeliveryTag();
            try {
                byte[] body = delivery.getBody();
                ResultMessage result = OBJECT_MAPPER.readValue(body, ResultMessage.class);

                if (result == null) {
                    System.err.println("Aggregator received null result, skipping");
                    channel.basicAck(deliveryTag, false);
                    return;
                }

                handleResult(result, channel);

                channel.basicAck(deliveryTag, false);
            } catch (Exception ex) {
                System.err.println("Aggregator failed to process message, will requeue");
                ex.printStackTrace(System.err);
                channel.basicNack(deliveryTag, false, true);
            }
        };

        boolean autoAck = false;
        channel.basicConsume(RESULT_QUEUE_NAME, autoAck, deliverCallback, consumerTag -> {
            System.out.println("Aggregator " + aggregatorId + " cancelled consumer: " + consumerTag);
        });
    }

    /**
     * Обработка одного ResultMessage: обновление агрегатов по jobId.
     */
    private static void handleResult(ResultMessage result, Channel channel) {
        String jobId = result.getJobId();
        int sectionIndex = result.getSectionIndex();
        int totalSections = result.getTotalSections();

        if (jobId == null) {
            System.err.println("Aggregator: received message without jobId, skipping");
            return;
        }

        JobAggregation job = JOBS.computeIfAbsent(jobId, id -> new JobAggregation(id, totalSections));

        synchronized (job) {
            if (job.getTotalSections() != totalSections) {
                System.err.println("Aggregator: inconsistent totalSections for job " + jobId +
                        ": existing=" + job.getTotalSections() +
                        ", new=" + totalSections);
            }

            if (job.hasSection(sectionIndex)) {
                System.out.println("Aggregator: duplicate section " + sectionIndex +
                        " for job " + jobId + ", ignoring");
                return;
            }

            job.addSection(result);

            System.out.println("Aggregator: received section " + sectionIndex +
                    " of " + job.getTotalSections() +
                    " for job " + jobId +
                    ", wordCount = " + result.getWordCount());

            if (job.isComplete()) {
                finalizeJob(job, channel);
                JOBS.remove(jobId);
            }
        }
    }

    /**
     * Финализация job-а: печать итоговой статистики и отправка FinalJobResult в очередь.
     */
    private static void finalizeJob(JobAggregation job, Channel channel) {
        List<ResultMessage.WordFrequency> globalTop =
                buildTopN(job.getGlobalWordFrequencies(), GLOBAL_TOP_N);

        System.out.println();
        System.out.println("======== Aggregated result for job " + job.getJobId() + " ========");
        System.out.println("Sections:       " + job.getTotalSections());
        System.out.println("Total wordCount " + job.getTotalWordCount());
        System.out.println();
        System.out.println("Top " + GLOBAL_TOP_N + " words (merged from all sections):");

        for (ResultMessage.WordFrequency wf : globalTop) {
            System.out.printf("  %-20s -> %d%n", wf.getWord(), wf.getCount());
        }

        System.out.println();
        System.out.println("Per-section summary:");
        List<ResultMessage> sections = new ArrayList<>(job.getSections().values());
        Collections.sort(sections, Comparator.comparingInt(ResultMessage::getSectionIndex));
        for (ResultMessage section : sections) {
            System.out.println("  Section " + section.getSectionIndex() +
                    ": wordCount=" + section.getWordCount());
        }

        System.out.println("======== End of aggregated result for job " + job.getJobId() + " ========");
        System.out.println();

        try {
            FinalJobResult finalResult = new FinalJobResult();
            finalResult.setJobId(job.getJobId());
            finalResult.setTotalSections(job.getTotalSections());
            finalResult.setTotalWordCount(job.getTotalWordCount());
            finalResult.setGlobalTopWords(globalTop);
            finalResult.setSections(sections);

            byte[] body = OBJECT_MAPPER.writeValueAsBytes(finalResult);

            channel.basicPublish("", FINAL_RESULT_QUEUE_NAME, null, body);

            System.out.println("Aggregator: published FinalJobResult for job " +
                    job.getJobId() + " to queue '" + FINAL_RESULT_QUEUE_NAME + "'");
        } catch (IOException e) {
            System.err.println("Aggregator: failed to publish FinalJobResult for job " + job.getJobId());
            e.printStackTrace(System.err);
        }
    }

    /**
     * Построение глобального топ-N из карты частот.
     */
    private static List<ResultMessage.WordFrequency> buildTopN(Map<String, Integer> freqMap, int limit) {
        if (freqMap.isEmpty()) {
            return Collections.emptyList();
        }

        return freqMap.entrySet()
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
                .limit(limit)
                .map(entry -> new ResultMessage.WordFrequency(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    private static String buildAggregatorId() {
        String threadPart = Thread.currentThread().getName();
        String randomPart = java.util.UUID.randomUUID().toString().substring(0, 8);
        return threadPart + "-" + randomPart;
    }

    /**
     * Состояние агрегации по одному jobId.
     */
    private static final class JobAggregation {
        private final String jobId;
        private final Map<Integer, ResultMessage> sections;
        private final Map<String, Integer> globalWordFrequencies;
        private final Set<Integer> receivedSectionIndexes;

        private int totalSections;
        private int receivedSections;
        private int totalWordCount;

        JobAggregation(String jobId, int totalSections) {
            this.jobId = jobId;
            this.sections = new HashMap<>();
            this.globalWordFrequencies = new HashMap<>();
            this.receivedSectionIndexes = ConcurrentHashMap.newKeySet();
            this.totalSections = totalSections;
            this.receivedSections = 0;
            this.totalWordCount = 0;
        }

        String getJobId() {
            return jobId;
        }

        int getTotalSections() {
            return totalSections;
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

        boolean hasSection(int sectionIndex) {
            return receivedSectionIndexes.contains(sectionIndex);
        }

        boolean isComplete() {
            return totalSections > 0 && receivedSections >= totalSections;
        }

        void addSection(ResultMessage result) {
            int sectionIndex = result.getSectionIndex();
            receivedSectionIndexes.add(sectionIndex);
            sections.put(sectionIndex, result);

            receivedSections += 1;
            totalWordCount += result.getWordCount();

            List<ResultMessage.WordFrequency> topWords = result.getTopWords();
            if (topWords != null) {
                for (ResultMessage.WordFrequency wf : topWords) {
                    if (wf.getWord() == null) {
                        continue;
                    }
                    globalWordFrequencies.merge(wf.getWord(), wf.getCount(), Integer::sum);
                }
            }
        }
    }
}
