package main;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateValue implements ComparableValue , java.io.Serializable {
    private final Date value;
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    public DateValue(String value) {
        try {
            this.value = format.parse(value);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid date format. Use yyyy-MM-dd.");
        }
    }

    @Override
    public int compare(ComparableValue other) {
        return this.value.compareTo(((DateValue) other).value);
    }

    @Override
    public Date getValue() {
        return value;
    }
}
