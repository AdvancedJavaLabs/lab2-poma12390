package itmo.maga.javaparallel.lab2.common;

import java.util.ArrayList;
import java.util.List;

public final class ResultMessage {

    private String jobId;
    private int sectionIndex;
    private int totalSections;
    private int wordCount;
    private List<WordFrequency> topWords;

    public ResultMessage() {
        this.topWords = new ArrayList<>();
    }

    public ResultMessage(String jobId,
                         int sectionIndex,
                         int totalSections,
                         int wordCount,
                         List<WordFrequency> topWords) {
        this.jobId = jobId;
        this.sectionIndex = sectionIndex;
        this.totalSections = totalSections;
        this.wordCount = wordCount;
        this.topWords = topWords != null ? topWords : new ArrayList<>();
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
        this.topWords = topWords != null ? topWords : new ArrayList<>();
    }

    @Override
    public String toString() {
        return "ResultMessage{" +
                "jobId='" + jobId + '\'' +
                ", sectionIndex=" + sectionIndex +
                ", totalSections=" + totalSections +
                ", wordCount=" + wordCount +
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
