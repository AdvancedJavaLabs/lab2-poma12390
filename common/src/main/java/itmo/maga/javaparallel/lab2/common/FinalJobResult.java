package itmo.maga.javaparallel.lab2.common;

import java.util.ArrayList;
import java.util.List;

public final class FinalJobResult {

    private String jobId;
    private int totalSections;
    private int totalWordCount;
    private List<ResultMessage.WordFrequency> globalTopWords;
    private List<ResultMessage> sections;

    public FinalJobResult() {
        this.globalTopWords = new ArrayList<>();
        this.sections = new ArrayList<>();
    }

    public FinalJobResult(
            String jobId,
            int totalSections,
            int totalWordCount,
            List<ResultMessage.WordFrequency> globalTopWords,
            List<ResultMessage> sections
    ) {
        this.jobId = jobId;
        this.totalSections = totalSections;
        this.totalWordCount = totalWordCount;
        this.globalTopWords = globalTopWords != null ? new ArrayList<>(globalTopWords) : new ArrayList<>();
        this.sections = sections != null ? new ArrayList<>(sections) : new ArrayList<>();
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

    @Override
    public String toString() {
        return "FinalJobResult{" +
                "jobId='" + jobId + '\'' +
                ", totalSections=" + totalSections +
                ", totalWordCount=" + totalWordCount +
                ", globalTopWordsSize=" + (globalTopWords != null ? globalTopWords.size() : 0) +
                ", sectionsCount=" + (sections != null ? sections.size() : 0) +
                '}';
    }
}
