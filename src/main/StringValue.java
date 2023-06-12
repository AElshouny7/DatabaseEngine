package main;

public class StringValue implements ComparableValue , java.io.Serializable {
    private final String value;

    public StringValue(String value) {
        this.value = value;
    }

    @Override
    public int compare(ComparableValue other) {
        return this.value.compareTo(((StringValue) other).value);
    }

    @Override
    public String getValue() {
        return value;
    }
}
