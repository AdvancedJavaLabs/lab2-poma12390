package itmo.maga.javaparallel.lab2.common;

import java.util.ArrayList;
import java.util.List;

public final class ResultMessage {

    private String jobId;
    private int sectionIndex;
    private int totalSections;
    private int wordCount;
    private List<WordFrequency> topWords;

    private int sentimentScore;
    private int positiveWordCount;
    private int negativeWordCount;

    private String transformedSectionText;

    public ResultMessage() {
        this.topWords = new ArrayList<>();
    }

    public ResultMessage(
            String jobId,
            int sectionIndex,
            int totalSections,
            int wordCount,
            List<WordFrequency> topWords
    ) {
        this(jobId, sectionIndex, totalSections, wordCount, topWords,
                0, 0, 0, null);
    }

    public ResultMessage(
            String jobId,
            int sectionIndex,
            int totalSections,
            int wordCount,
            List<WordFrequency> topWords,
            int sentimentScore,
            int positiveWordCount,
            int negativeWordCount
    ) {
        this(jobId, sectionIndex, totalSections, wordCount, topWords,
                sentimentScore, positiveWordCount, negativeWordCount, null);
    }

    public ResultMessage(
            String jobId,
            int sectionIndex,
            int totalSections,
            int wordCount,
            List<WordFrequency> topWords,
            int sentimentScore,
            int positiveWordCount,
            int negativeWordCount,
            String transformedSectionText
    ) {
        this.jobId = jobId;
        this.sectionIndex = sectionIndex;
        this.totalSections = totalSections;
        this.wordCount = wordCount;
        this.topWords = topWords != null ? new ArrayList<>(topWords) : new ArrayList<>();
        this.sentimentScore = sentimentScore;
        this.positiveWordCount = positiveWordCount;
        this.negativeWordCount = negativeWordCount;
        this.transformedSectionText = transformedSectionText;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public int getSectionIndex() {
        return sectionIndex;
    }

    public void setSectionIndex(int sectionIndex) {
        this.sectionIndex = sectionIndex;
    }

    public int getTotalSections() {
        return totalSections;
    }

    public void setTotalSections(int totalSections) {
        this.totalSections = totalSections;
    }

    public int getWordCount() {
        return wordCount;
    }

    public void setWordCount(int wordCount) {
        this.wordCount = wordCount;
    }

    public List<WordFrequency> getTopWords() {
        return topWords;
    }

    public void setTopWords(List<WordFrequency> topWords) {
        this.topWords = topWords != null ? new ArrayList<>(topWords) : new ArrayList<>();
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

    public String getTransformedSectionText() {
        return transformedSectionText;
    }

    public void setTransformedSectionText(String transformedSectionText) {
        this.transformedSectionText = transformedSectionText;
    }

    @Override
    public String toString() {
        return "ResultMessage{" +
                "jobId='" + jobId + '\'' +
                ", sectionIndex=" + sectionIndex +
                ", totalSections=" + totalSections +
                ", wordCount=" + wordCount +
                ", sentimentScore=" + sentimentScore +
                ", positiveWordCount=" + positiveWordCount +
                ", negativeWordCount=" + negativeWordCount +
                ", transformedSectionTextLength=" +
                (transformedSectionText != null ? transformedSectionText.length() : 0) +
                ", topWords=" + topWords +
                '}';
    }

    public static final class WordFrequency {

        private String word;
        private int count;

        public WordFrequency() {
        }

        public WordFrequency(String word, int count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordFrequency{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
