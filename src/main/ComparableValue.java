package main;

public interface ComparableValue  {
    int compare(ComparableValue other);

    Object getValue();

    boolean equals(Object obj);
}
