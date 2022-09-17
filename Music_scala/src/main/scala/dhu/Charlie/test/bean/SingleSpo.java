package dhu.Charlie.test.bean;


public class SingleSpo {
    private String predicate;
    private String object_type;
    private String subject_type;
    private String object;
    private String subject;

    public SingleSpo(String predicate, String object_type, String subject_type, String object, String subject) {
        this.predicate = predicate;
        this.object_type = object_type;
        this.subject_type = subject_type;
        this.object = object;
        this.subject = subject;
    }

    public String getPredicate() {
        return predicate;
    }

    public String getObject_type() {
        return object_type;
    }

    public String getSubject_type() {
        return subject_type;
    }

    public String getObject() {
        return object;
    }

    public String getSubject() {
        return subject;
    }

    public String toString() {
        return "{\"predicate\":\"" + predicate + "\", \"object_type\":\"" + object_type + "\", \"subject_type\":\"" + subject_type + "\", \"object\":\"" + object + "\", \"subject\":\"" + subject + "\"}";
    }
}
