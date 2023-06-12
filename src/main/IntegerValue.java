package main;

public class IntegerValue implements ComparableValue , java.io.Serializable {
    private final Integer value;

    public IntegerValue(Integer value) {
        this.value = value;
    }

    @Override
    public int compare(ComparableValue other) {
        return this.value.compareTo(((IntegerValue) other).value);
    }

    @Override
    public Integer getValue() {
        return value;
    }
}
