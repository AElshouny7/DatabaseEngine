package main;

public class DoubleValue implements ComparableValue , java.io.Serializable {
    private final Double value;

    public DoubleValue(Double value) {
        this.value = value;
    }

    @Override
    public int compare(ComparableValue other) {
        return this.value.compareTo(((DoubleValue) other).value);
    }

    @Override
    public Double getValue() {
        return value;
    }
}
