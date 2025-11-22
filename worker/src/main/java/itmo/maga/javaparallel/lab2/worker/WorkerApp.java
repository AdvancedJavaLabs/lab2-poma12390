package itmo.maga.javaparallel.lab2.worker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import itmo.maga.javaparallel.lab2.common.ResultMessage;
import itmo.maga.javaparallel.lab2.common.TaskMessage;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WorkerApp {

    private static final String TASK_QUEUE_NAME = "text_tasks";
    private static final String RESULT_QUEUE_NAME = "text_results";

    private static final String RABBIT_HOST = "localhost";
    private static final int RABBIT_PORT = 5672;
    private static final String RABBIT_USERNAME = "labuser";
    private static final String RABBIT_PASSWORD = "labpassword";

    private static final String SENTIMENT_LEXICON_RESOURCE = "sentiment_lexicon.json";
    private static final String NAME_REPLACEMENTS_RESOURCE = "name_replacements.json";

    private static final Set<String> POSITIVE_WORDS;
    private static final Set<String> NEGATIVE_WORDS;

    private static final List<NameReplacementRule> NAME_REPLACEMENT_RULES;

    static {
        ObjectMapper mapper = new ObjectMapper();
        Set<String> positive = new HashSet<>();
        Set<String> negative = new HashSet<>();
        List<NameReplacementRule> replacementRules = new ArrayList<>();

        try (InputStream in = WorkerApp.class
                .getClassLoader()
                .getResourceAsStream(SENTIMENT_LEXICON_RESOURCE)) {

            if (in == null) {
                throw new IllegalStateException(
                        "Sentiment lexicon resource not found on classpath: " + SENTIMENT_LEXICON_RESOURCE
                );
            }

            SentimentLexiconConfig config = mapper.readValue(in, SentimentLexiconConfig.class);

            if (config.getPositive() != null) {
                for (String word : config.getPositive()) {
                    if (word == null) {
                        continue;
                    }
                    String normalized = word.trim().toLowerCase();
                    if (!normalized.isEmpty()) {
                        positive.add(normalized);
                    }
                }
            }

            if (config.getNegative() != null) {
                for (String word : config.getNegative()) {
                    if (word == null) {
                        continue;
                    }
                    String normalized = word.trim().toLowerCase();
                    if (!normalized.isEmpty()) {
                        negative.add(normalized);
                    }
                }
            }
        } catch (IOException e) {
            throw new ExceptionInInitializerError(
                    "Failed to load sentiment lexicon from resource " +
                            SENTIMENT_LEXICON_RESOURCE + ": " + e.getMessage()
            );
        }

        try (InputStream in = WorkerApp.class
                .getClassLoader()
                .getResourceAsStream(NAME_REPLACEMENTS_RESOURCE)) {

            if (in == null) {
                System.out.println(
                        "Name replacements resource not found on classpath: " +
                                NAME_REPLACEMENTS_RESOURCE +
                                ". Name replacement will be disabled."
                );
            } else {
                Map<String, String> raw = mapper.readValue(
                        in,
                        new TypeReference<Map<String, String>>() {
                        }
                );
                if (raw != null) {
                    for (Map.Entry<String, String> entry : raw.entrySet()) {
                        String from = entry.getKey();
                        String to = entry.getValue();
                        if (from == null || to == null) {
                            continue;
                        }
                        String source = from.trim();
                        String target = to.trim();
                        if (source.isEmpty() || target.isEmpty()) {
                            continue;
                        }
                        // \bName\b — замена только целого слова
                        String regex = "\\b" + Pattern.quote(source) + "\\b";
                        Pattern pattern = Pattern.compile(regex);
                        replacementRules.add(new NameReplacementRule(pattern, target));
                    }
                }
            }
        } catch (IOException e) {
            throw new ExceptionInInitializerError(
                    "Failed to load name replacements from resource " +
                            NAME_REPLACEMENTS_RESOURCE + ": " + e.getMessage()
            );
        }

        POSITIVE_WORDS = Collections.unmodifiableSet(positive);
        NEGATIVE_WORDS = Collections.unmodifiableSet(negative);
        NAME_REPLACEMENT_RULES = Collections.unmodifiableList(replacementRules);

        System.out.println(
                "Loaded sentiment lexicon: " +
                        POSITIVE_WORDS.size() + " positive words, " +
                        NEGATIVE_WORDS.size() + " negative words"
        );
        System.out.println(
                "Loaded name replacement rules: " + NAME_REPLACEMENT_RULES.size()
        );
    }

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

        System.out.println(
                "Worker " + workerId +
                        " started. Waiting for messages from '" + TASK_QUEUE_NAME + "'"
        );

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            long deliveryTag = delivery.getEnvelope().getDeliveryTag();
            try {
                byte[] body = delivery.getBody();
                TaskMessage task = objectMapper.readValue(body, TaskMessage.class);

                if (task == null) {
                    System.err.println("Worker " + workerId + " got null TaskMessage, skipping");
                    channel.basicAck(deliveryTag, false);
                    return;
                }

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
                                ", wordCount = " + result.getWordCount() +
                                ", sentimentScore = " + result.getSentimentScore() +
                                ", positiveWords = " + result.getPositiveWordCount() +
                                ", negativeWords = " + result.getNegativeWordCount()
                );

                channel.basicAck(deliveryTag, false);
            } catch (Exception ex) {
                System.err.println(
                        "Worker " + workerId +
                                " failed to process message, will requeue"
                );
                ex.printStackTrace(System.err);
                channel.basicNack(deliveryTag, false, true);
            }
        };

        boolean autoAck = false;
        channel.basicConsume(
                TASK_QUEUE_NAME,
                autoAck,
                deliverCallback,
                consumerTag -> System.out.println(
                        "Worker " + workerId + " cancelled consumer: " + consumerTag
                )
        );
    }

    private static String buildWorkerId() {
        String threadPart = Thread.currentThread().getName();
        String randomPart = UUID.randomUUID().toString().substring(0, 8);
        return threadPart + "-" + randomPart;
    }

    private static ResultMessage processTask(TaskMessage task) {
        String originalText = task.getSectionText();
        if (originalText == null) {
            originalText = "";
        }

        String transformedText = applyNameReplacements(originalText);

        List<String> tokens = tokenize(transformedText);
        int wordCount = tokens.size();

        Map<String, Integer> freqMap = new HashMap<>();

        int positiveCount = 0;
        int negativeCount = 0;
        int sentimentScore = 0;

        for (String token : tokens) {
            if (token == null || token.isEmpty()) {
                continue;
            }

            freqMap.merge(token, 1, Integer::sum);

            if (isPositiveWord(token)) {
                positiveCount++;
                sentimentScore++;
            } else if (isNegativeWord(token)) {
                negativeCount++;
                sentimentScore--;
            }
        }

        List<ResultMessage.WordFrequency> topWords = freqMap.entrySet()
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

        ResultMessage result = new ResultMessage();
        result.setJobId(task.getJobId());
        result.setSectionIndex(task.getSectionIndex());
        result.setTotalSections(task.getTotalSections());
        result.setWordCount(wordCount);
        result.setTopWords(topWords);
        result.setSentimentScore(sentimentScore);
        result.setPositiveWordCount(positiveCount);
        result.setNegativeWordCount(negativeCount);
        result.setTransformedSectionText(transformedText);

        return result;
    }

    private static String applyNameReplacements(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        if (NAME_REPLACEMENT_RULES.isEmpty()) {
            return text;
        }
        String result = text;
        for (NameReplacementRule rule : NAME_REPLACEMENT_RULES) {
            Matcher matcher = rule.getPattern().matcher(result);
            result = matcher.replaceAll(rule.getReplacement());
        }
        return result;
    }

    private static boolean isPositiveWord(String word) {
        return word != null && POSITIVE_WORDS.contains(word);
    }

    private static boolean isNegativeWord(String word) {
        return word != null && NEGATIVE_WORDS.contains(word);
    }

    private static List<String> tokenize(String text) {
        List<String> tokens = new ArrayList<>();
        if (text == null || text.isEmpty()) {
            return tokens;
        }

        String normalized = text.toLowerCase();
        String[] parts = normalized.split("[^\\p{L}\\p{Nd}]+");

        for (String part : parts) {
            if (part != null) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    tokens.add(trimmed);
                }
            }
        }

        return tokens;
    }

    private static final class SentimentLexiconConfig {

        private List<String> positive;
        private List<String> negative;

        public SentimentLexiconConfig() {
        }

        public List<String> getPositive() {
            return positive;
        }

        public void setPositive(List<String> positive) {
            this.positive = positive;
        }

        public List<String> getNegative() {
            return negative;
        }

        public void setNegative(List<String> negative) {
            this.negative = negative;
        }
    }

    // Правило замены имени
    private static final class NameReplacementRule {

        private final Pattern pattern;
        private final String replacement;

        NameReplacementRule(Pattern pattern, String replacement) {
            this.pattern = pattern;
            this.replacement = replacement;
        }

        Pattern getPattern() {
            return pattern;
        }

        String getReplacement() {
            return replacement;
        }
    }
}
