package itmo.maga.javaparallel.lab2.common;

import java.util.ArrayList;
import java.util.List;

public final class FinalJobResult {

    private String jobId;
    private int totalSections;
    private int totalWordCount;
    private List<ResultMessage.WordFrequency> globalTopWords;
    private List<ResultMessage> sections;

    private int totalSentimentScore;
    private int totalPositiveWordCount;
    private int totalNegativeWordCount;
    private double averageSentimentPerSection;

    private String modifiedText;

    private List<String> sortedSentences;

    public FinalJobResult() {
        this.globalTopWords = new ArrayList<>();
        this.sections = new ArrayList<>();
        this.sortedSentences = new ArrayList<>();
    }

    public FinalJobResult(
            String jobId,
            int totalSections,
            int totalWordCount,
            List<ResultMessage.WordFrequency> globalTopWords,
            List<ResultMessage> sections,
            int totalSentimentScore,
            int totalPositiveWordCount,
            int totalNegativeWordCount,
            double averageSentimentPerSection,
            String modifiedText
    ) {
        this.jobId = jobId;
        this.totalSections = totalSections;
        this.totalWordCount = totalWordCount;
        this.globalTopWords = globalTopWords != null ? new ArrayList<>(globalTopWords) : new ArrayList<>();
        this.sections = sections != null ? new ArrayList<>(sections) : new ArrayList<>();
        this.totalSentimentScore = totalSentimentScore;
        this.totalPositiveWordCount = totalPositiveWordCount;
        this.totalNegativeWordCount = totalNegativeWordCount;
        this.averageSentimentPerSection = averageSentimentPerSection;
        this.modifiedText = modifiedText;
        this.sortedSentences = new ArrayList<>();
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
        this.globalTopWords = globalTopWords != null ? new ArrayList<>(globalTopWords) : new ArrayList<>();
    }

    public List<ResultMessage> getSections() {
        return sections;
    }

    public void setSections(List<ResultMessage> sections) {
        this.sections = sections != null ? new ArrayList<>(sections) : new ArrayList<>();
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

    public String getModifiedText() {
        return modifiedText;
    }

    public void setModifiedText(String modifiedText) {
        this.modifiedText = modifiedText;
    }

    public List<String> getSortedSentences() {
        return sortedSentences;
    }

    public void setSortedSentences(List<String> sortedSentences) {
        this.sortedSentences = sortedSentences != null ? new ArrayList<>(sortedSentences) : new ArrayList<>();
    }

    @Override
    public String toString() {
        return "FinalJobResult{" +
                "jobId='" + jobId + '\'' +
                ", totalSections=" + totalSections +
                ", totalWordCount=" + totalWordCount +
                ", globalTopWordsSize=" + (globalTopWords != null ? globalTopWords.size() : 0) +
                ", sectionsCount=" + (sections != null ? sections.size() : 0) +
                ", totalSentimentScore=" + totalSentimentScore +
                ", totalPositiveWordCount=" + totalPositiveWordCount +
                ", totalNegativeWordCount=" + totalNegativeWordCount +
                ", averageSentimentPerSection=" + averageSentimentPerSection +
                ", modifiedTextLength=" + (modifiedText != null ? modifiedText.length() : 0) +
                ", sortedSentencesCount=" + (sortedSentences != null ? sortedSentences.size() : 0) +
                '}';
    }
}
