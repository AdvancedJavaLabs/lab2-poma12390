package itmo.maga.javaparallel.lab2.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import itmo.maga.javaparallel.lab2.common.FinalJobResult;
import itmo.maga.javaparallel.lab2.common.ResultMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class ResultSinkApp {

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
        }
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        return mapper;
    }

    private static void runSink() throws IOException, TimeoutException {
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

                Path jsonOutputPath = buildJsonOutputPath(result);
                writeJsonResultToFile(result, jsonOutputPath);

                Path textOutputPath = null;
                String modifiedText = result.getModifiedText();
                if (modifiedText != null && !modifiedText.isEmpty()) {
                    textOutputPath = buildModifiedTextOutputPath(result);
                    writeModifiedTextToFile(modifiedText, textOutputPath);
                }

                Path sortedSentencesOutputPath = null;
                List<String> sortedSentences = result.getSortedSentences();
                if (sortedSentences != null && !sortedSentences.isEmpty()) {
                    sortedSentencesOutputPath = buildSortedSentencesOutputPath(result);
                    writeSortedSentencesToFile(sortedSentences, sortedSentencesOutputPath);
                }

                if (textOutputPath != null || sortedSentencesOutputPath != null) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("Result sink ").append(sinkId)
                            .append(" saved result for job ").append(result.getJobId())
                            .append(" to ").append(jsonOutputPath.toAbsolutePath());
                    if (textOutputPath != null) {
                        sb.append(" and ").append(textOutputPath.toAbsolutePath());
                    }
                    if (sortedSentencesOutputPath != null) {
                        sb.append(" and ").append(sortedSentencesOutputPath.toAbsolutePath());
                    }
                    System.out.println(sb.toString());
                } else {
                    System.out.println(
                            "Result sink " + sinkId +
                                    " saved result for job " + result.getJobId() +
                                    " to " + jsonOutputPath.toAbsolutePath() +
                                    " (no modifiedText or sorted sentences to write)"
                    );
                }

                channel.basicAck(deliveryTag, false);
            } catch (Exception ex) {
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

    private static Path buildJsonOutputPath(FinalJobResult result) {
        String jobId = result.getJobId() != null ? result.getJobId() : "unknown";
        String fileName = "job-" + jobId + ".json";
        return OUTPUT_DIR.resolve(fileName);
    }

    private static Path buildModifiedTextOutputPath(FinalJobResult result) {
        String jobId = result.getJobId() != null ? result.getJobId() : "unknown";
        String fileName = "job-" + jobId + "-modified.txt";
        return OUTPUT_DIR.resolve(fileName);
    }

    private static Path buildSortedSentencesOutputPath(FinalJobResult result) {
        String jobId = result.getJobId() != null ? result.getJobId() : "unknown";
        String fileName = "job-" + jobId + "-sentences-sorted.txt";
        return OUTPUT_DIR.resolve(fileName);
    }

    private static void writeJsonResultToFile(FinalJobResult result, Path outputPath) throws IOException {
        FinalJobResultWithoutText dto = new FinalJobResultWithoutText();
        dto.setJobId(result.getJobId());
        dto.setTotalSections(result.getTotalSections());
        dto.setTotalWordCount(result.getTotalWordCount());
        dto.setGlobalTopWords(result.getGlobalTopWords());
        dto.setSections(convertSections(result.getSections()));
        dto.setTotalSentimentScore(result.getTotalSentimentScore());
        dto.setTotalPositiveWordCount(result.getTotalPositiveWordCount());
        dto.setTotalNegativeWordCount(result.getTotalNegativeWordCount());
        dto.setAverageSentimentPerSection(result.getAverageSentimentPerSection());

        OBJECT_MAPPER.writeValue(outputPath.toFile(), dto);
    }

    private static List<SectionStatsWithoutText> convertSections(List<ResultMessage> sections) {
        List<SectionStatsWithoutText> list = new ArrayList<>();
        if (sections == null) {
            return list;
        }
        for (ResultMessage section : sections) {
            if (section == null) {
                continue;
            }
            SectionStatsWithoutText stats = new SectionStatsWithoutText();
            stats.setSectionIndex(section.getSectionIndex());
            stats.setWordCount(section.getWordCount());
            stats.setSentimentScore(section.getSentimentScore());
            stats.setPositiveWordCount(section.getPositiveWordCount());
            stats.setNegativeWordCount(section.getNegativeWordCount());
            stats.setTopWords(section.getTopWords());
            list.add(stats);
        }
        return list;
    }

    private static void writeModifiedTextToFile(String modifiedText, Path outputPath) throws IOException {
        byte[] bytes = modifiedText.getBytes(StandardCharsets.UTF_8);
        Files.write(outputPath, bytes);
    }

    private static void writeSortedSentencesToFile(List<String> sortedSentences, Path outputPath) throws IOException {
        List<String> lines = new ArrayList<>();
        if (sortedSentences != null) {
            for (String sentence : sortedSentences) {
                if (sentence == null) {
                    continue;
                }
                lines.add(sentence);
            }
        }
        Files.write(outputPath, lines, StandardCharsets.UTF_8);
    }

    private static String buildSinkId() {
        String threadPart = Thread.currentThread().getName();
        String randomPart = UUID.randomUUID().toString().substring(0, 8);
        return threadPart + "-" + randomPart;
    }

    private static final class FinalJobResultWithoutText {

        private String jobId;
        private int totalSections;
        private int totalWordCount;
        private List<ResultMessage.WordFrequency> globalTopWords;
        private List<SectionStatsWithoutText> sections;
        private int totalSentimentScore;
        private int totalPositiveWordCount;
        private int totalNegativeWordCount;
        private double averageSentimentPerSection;

        public FinalJobResultWithoutText() {
        }

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public int getTotalSections() {
            return totalSections;
        }

        public void setTotalSections(int totalSections) {
            this.totalSections = totalSections;
        }

        public int getTotalWordCount() {
            return totalWordCount;
        }

        public void setTotalWordCount(int totalWordCount) {
            this.totalWordCount = totalWordCount;
        }

        public List<ResultMessage.WordFrequency> getGlobalTopWords() {
            return globalTopWords;
        }

        public void setGlobalTopWords(List<ResultMessage.WordFrequency> globalTopWords) {
            this.globalTopWords = globalTopWords;
        }

        public List<SectionStatsWithoutText> getSections() {
            return sections;
        }

        public void setSections(List<SectionStatsWithoutText> sections) {
            this.sections = sections;
        }

        public int getTotalSentimentScore() {
            return totalSentimentScore;
        }

        public void setTotalSentimentScore(int totalSentimentScore) {
            this.totalSentimentScore = totalSentimentScore;
        }

        public int getTotalPositiveWordCount() {
            return totalPositiveWordCount;
        }

        public void setTotalPositiveWordCount(int totalPositiveWordCount) {
            this.totalPositiveWordCount = totalPositiveWordCount;
        }

        public int getTotalNegativeWordCount() {
            return totalNegativeWordCount;
        }

        public void setTotalNegativeWordCount(int totalNegativeWordCount) {
            this.totalNegativeWordCount = totalNegativeWordCount;
        }

        public double getAverageSentimentPerSection() {
            return averageSentimentPerSection;
        }

        public void setAverageSentimentPerSection(double averageSentimentPerSection) {
            this.averageSentimentPerSection = averageSentimentPerSection;
        }
    }

    /**
     * Статистика по секции без текстового содержимого.
     */
    private static final class SectionStatsWithoutText {

        private int sectionIndex;
        private int wordCount;
        private int sentimentScore;
        private int positiveWordCount;
        private int negativeWordCount;
        private List<ResultMessage.WordFrequency> topWords;

        public SectionStatsWithoutText() {
        }

        public int getSectionIndex() {
            return sectionIndex;
        }

        public void setSectionIndex(int sectionIndex) {
            this.sectionIndex = sectionIndex;
        }

        public int getWordCount() {
            return wordCount;
        }

        public void setWordCount(int wordCount) {
            this.wordCount = wordCount;
        }

        public int getSentimentScore() {
            return sentimentScore;
        }

        public void setSentimentScore(int sentimentScore) {
            this.sentimentScore = sentimentScore;
        }

        public int getPositiveWordCount() {
            return positiveWordCount;
        }

        public void setPositiveWordCount(int positiveWordCount) {
            this.positiveWordCount = positiveWordCount;
        }

        public int getNegativeWordCount() {
            return negativeWordCount;
        }

        public void setNegativeWordCount(int negativeWordCount) {
            this.negativeWordCount = negativeWordCount;
        }

        public List<ResultMessage.WordFrequency> getTopWords() {
            return topWords;
        }

        public void setTopWords(List<ResultMessage.WordFrequency> topWords) {
            this.topWords = topWords;
        }
    }
}
