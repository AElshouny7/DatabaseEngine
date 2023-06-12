package main;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Octree implements Serializable {
    private OctreeNode root;
    private String[] strarrColName;

    private ComparableValue minX, maxX, minY, maxY, minZ, maxZ;

    public Octree(String typeX, String typeY, String typeZ, String minX, String maxX, String minY, String maxY,
            String minZ, String maxZ, String[] strarrColName) throws IOException {

        this.minX = createComparableValue(typeX, minX);
        this.maxX = createComparableValue(typeX, maxX);
        this.minY = createComparableValue(typeY, minY);
        this.maxY = createComparableValue(typeY, maxY);
        this.minZ = createComparableValue(typeZ, minZ);
        this.maxZ = createComparableValue(typeZ, maxZ);

        this.strarrColName = strarrColName;

        this.root = new OctreeNode(this.minX, this.maxX, this.minY, this.maxY, this.minZ, this.maxZ);

    }

    public void insert(Object x, Object y, Object z, String pageID) throws IOException {
        ComparableValue xValue = createComparableValue(x);
        ComparableValue yValue = createComparableValue(y);
        ComparableValue zValue = createComparableValue(z);
        root.insert(xValue, yValue, zValue, pageID);
    }

    public void update(Object x, Object y, Object z, String newPageID) throws IOException {
        delete(x, y, z);
        insert(x, y, z, newPageID);
    }

    public boolean delete(Object x, Object y, Object z) {
        ComparableValue xValue = createComparableValue(x);
        ComparableValue yValue = createComparableValue(y);
        ComparableValue zValue = createComparableValue(z);
        return root.delete(xValue, yValue, zValue);
    }

    public ArrayList<String> exactQuery(Object x, Object y, Object z) {
        ComparableValue xValue = createComparableValue(x);
        ComparableValue yValue = createComparableValue(y);
        ComparableValue zValue = createComparableValue(z);
        if (root != null) {
            return root.exactQuery(xValue, yValue, zValue);
        }
        return new ArrayList<>(); // returning an empty list if the root is null
    }

    //
    public List<NodeEntry> rangeQuery(Object minX, Object maxX, Object minY, Object maxY, Object minZ, Object maxZ) {
        List<NodeEntry> results = new ArrayList<>();
        ComparableValue minXValue = createComparableValue(minX);
        ComparableValue maxXValue = createComparableValue(maxX);
        ComparableValue minYValue = createComparableValue(minY);
        ComparableValue maxYValue = createComparableValue(maxY);
        ComparableValue minZValue = createComparableValue(minZ);
        ComparableValue maxZValue = createComparableValue(maxZ);
        root.rangeQuery(results, minXValue, maxXValue, minYValue, maxYValue, minZValue, maxZValue);
        return results;
    }

    public ArrayList<String> runQuery(Object x, Object y, Object z, String operatorX, String operatorY,
            String operatorZ) {
        ComparableValue xValue = createComparableValue(x);
        ComparableValue yValue = createComparableValue(y);
        ComparableValue zValue = createComparableValue(z);
        if (root != null) {
            return root.runQuery(xValue, yValue, zValue, operatorX, operatorY, operatorZ);
        }
        return new ArrayList<>(); // returning an empty list if the root is null
    }

    public ComparableValue createComparableValue(String type, String value) {
        return switch (type) {
            case "java.lang.Integer" -> new IntegerValue(Integer.parseInt(value));
            case "java.lang.Double" -> new DoubleValue(Double.parseDouble(value));
            case "java.lang.String" -> new StringValue(value);
            case "java.util.Date" -> new DateValue(value);
            default -> throw new IllegalArgumentException("Unsupported type: " + type);
        };
    }

    public ComparableValue createComparableValue(Object value) {
        if (value instanceof Integer) {
            return new IntegerValue((Integer) value);
        } else if (value instanceof Double) {
            return new DoubleValue((Double) value);
        } else if (value instanceof String) {
            return new StringValue((String) value);
        } else if (value instanceof Date) {
            return new DateValue(((Date) value).toString());
        } else {
            throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName());
        }
    }

    public String[] getStrarrColName() {
        return strarrColName;
    }

    public OctreeNode getRoot() {
        return root;
    }

    public ComparableValue getMinX() {
        return minX;
    }

    public ComparableValue getMaxX() {
        return maxX;
    }

    public ComparableValue getMinY() {
        return minY;
    }

    public ComparableValue getMaxY() {
        return maxY;
    }

    public ComparableValue getMinZ() {
        return minZ;
    }

    public ComparableValue getMaxZ() {
        return maxZ;
    }

    

}
