package itmo.maga.javaparallel.lab2.common;

public final class TaskMessage {

    private String jobId;
    private int sectionIndex;
    private int totalSections;
    private String sectionText;

    public TaskMessage() {
    }

    public TaskMessage(String jobId, int sectionIndex, int totalSections, String sectionText) {
        this.jobId = jobId;
        this.sectionIndex = sectionIndex;
        this.totalSections = totalSections;
        this.sectionText = sectionText;
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

    public String getSectionText() {
        return sectionText;
    }

    public void setSectionText(String sectionText) {
        this.sectionText = sectionText;
    }

    @Override
    public String toString() {
        return "TaskMessage{" +
                "jobId='" + jobId + '\'' +
                ", sectionIndex=" + sectionIndex +
                ", totalSections=" + totalSections +
                ", sectionTextLength=" + (sectionText != null ? sectionText.length() : 0) +
                '}';
    }
}
