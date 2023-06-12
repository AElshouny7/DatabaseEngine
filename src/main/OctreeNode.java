package main;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class OctreeNode implements java.io.Serializable {
    public final List<NodeEntry> entries = new ArrayList<>();
    public final List<NodeEntry> overflowEntries = new ArrayList<>();
    public OctreeNode[] children;
    private ComparableValue minX, maxX, minY, maxY, minZ, maxZ;
    private static int MaximumEntriesinOctreeNode;

    public OctreeNode(ComparableValue minX, ComparableValue maxX, ComparableValue minY, ComparableValue maxY,
            ComparableValue minZ, ComparableValue maxZ) throws IOException {

        this.minX = minX;
        this.maxX = maxX;
        this.minY = minY;
        this.maxY = maxY;
        this.minZ = minZ;
        this.maxZ = maxZ;

        Properties props = new Properties();
        FileInputStream input = new FileInputStream(System.getProperty("user.dir") + File.separator + "DBApp.config");
        props.load(input);
        MaximumEntriesinOctreeNode = Integer.parseInt(props.getProperty("MaximumEntriesinOctreeNode"));
    }

    public void insert(ComparableValue x, ComparableValue y, ComparableValue z, String pageID) throws IOException {
        if (children != null) {
            insertIntoChildren(x, y, z, pageID);
            return;
        }

        NodeEntry newNodeEntry = new NodeEntry(x, y, z, pageID);
        if (entries.contains(newNodeEntry)) {
            overflowEntries.add(newNodeEntry);
        } else {
            entries.add(newNodeEntry);
        }

        if (entries.size() > MaximumEntriesinOctreeNode) {
            split(minX, maxX, minY, maxY, minZ, maxZ);
        }
    }

    public ArrayList<String> exactQuery(ComparableValue x, ComparableValue y, ComparableValue z) {
        // Start from root
        ArrayList<String> result = new ArrayList<>();
        OctreeNode currentNode = this;
        while (true) {
            // If current node is a leaf node, check all entries
            if (currentNode.isLeaf()) {
                for (NodeEntry entry : currentNode.entries) {
                    if (entry.getX().compare(x) == 0 && entry.getY().compare(y) == 0 && entry.getZ().compare(z) == 0) {
                        // Found the exact entry
                        result.add(entry.getPageID());
                    }
                }
                for (NodeEntry overflowEntry : currentNode.overflowEntries) {
                    if (overflowEntry.getX().compare(x) == 0 && overflowEntry.getY().compare(y) == 0
                            && overflowEntry.getZ().compare(z) == 0) {
                        // Found the exact overflow entry
                        result.add(overflowEntry.getPageID());
                    }
                }
                // Return the result
                return result;
            }
            // If current node is not a leaf node, find the correct child node to go into
            else {
                int octant = currentNode.getOctant(x, y, z);
                currentNode = currentNode.children[octant];
            }
        }
    }

    public void rangeQuery(List<NodeEntry> results, ComparableValue minX, ComparableValue maxX, ComparableValue minY,
            ComparableValue maxY, ComparableValue minZ, ComparableValue maxZ) {
        if (!intersects(minX, maxX, minY, maxY, minZ, maxZ)) {
            return;
        }

        for (NodeEntry entry : entries) {
            if (inRange(entry.getX(), minX, maxX) && inRange(entry.getY(), minY, maxY)
                    && inRange(entry.getZ(), minZ, maxZ)) {
                results.add(entry);
            }
        }

        for (NodeEntry entry : overflowEntries) {
            if (inRange(entry.getX(), minX, maxX) && inRange(entry.getY(), minY, maxY)
                    && inRange(entry.getZ(), minZ, maxZ)) {
                results.add(entry);
            }
        }

        if (children != null) {
            for (OctreeNode child : children) {
                child.rangeQuery(results, minX, maxX, minY, maxY, minZ, maxZ);
            }
        }
    }

    public ArrayList<String> runQuery(ComparableValue xValue, ComparableValue yValue, ComparableValue zValue,
            String operatorX, String operatorY, String operatorZ) {
        ArrayList<String> result = new ArrayList<>();
        if (children != null) {
            int octant = getOctant(xValue, yValue, zValue);
            result.addAll(children[octant].runQuery(xValue, yValue, zValue, operatorX, operatorY, operatorZ));
        }

        for (NodeEntry entry : entries) {
            if (compare(entry.getX(), xValue, operatorX) && compare(entry.getY(), yValue, operatorY)
                    && compare(entry.getZ(), zValue, operatorZ)) {
                result.add(entry.getPageID());
            }
        }

        for (NodeEntry entry : overflowEntries) {
            if (compare(entry.getX(), xValue, operatorX) && compare(entry.getY(), yValue, operatorY)
                    && compare(entry.getZ(), zValue, operatorZ)) {
                result.add(entry.getPageID());
            }
        }

        return result;
    }

    private boolean compare(ComparableValue x, ComparableValue xValue, String operatorX) {
        if (operatorX.equals("=")) {
            return x.compare(xValue) == 0;
        } else if (operatorX.equals(">")) {
            return x.compare(xValue) > 0;
        } else if (operatorX.equals("<")) {
            return x.compare(xValue) < 0;
        } else if (operatorX.equals(">=")) {
            return x.compare(xValue) >= 0;
        } else if (operatorX.equals("<=")) {
            return x.compare(xValue) <= 0;
        } else {
            return false;
        }
    }

    public boolean delete(ComparableValue x, ComparableValue y, ComparableValue z) {
        Iterator<NodeEntry> iterator = entries.iterator();
        boolean deleted = false;
        while (iterator.hasNext()) {
            NodeEntry entry = iterator.next();
            if (entry.getX().compare(x) == 0 && entry.getY().compare(y) == 0 && entry.getZ().compare(z) == 0) {
                iterator.remove();
                deleted = true;
            }
        }

        // Checking overflow entries
        iterator = overflowEntries.iterator();
        while (iterator.hasNext()) {
            NodeEntry entry = iterator.next();
            if (entry.getX().compare(x) == 0 && entry.getY().compare(y) == 0 && entry.getZ().compare(z) == 0) {
                iterator.remove();
                deleted = true;
            }
        }

        if (children != null) {
            int octant = getOctant(x, y, z);
            deleted = children[octant].delete(x, y, z);
            if (deleted) {
                if (areAllChildrenEmpty()) {
                    children = null;
                }
            }
            return deleted;
        }

        return deleted;
    }

    private void insertIntoChildren(ComparableValue x, ComparableValue y, ComparableValue z, String pageID)
            throws IOException {
        int octant = getOctant(x, y, z);
        children[octant].insert(x, y, z, pageID);
    }

    private int getOctant(ComparableValue x, ComparableValue y, ComparableValue z) {
        int octant = 0;
        octant |= getOctantHelper(x, minX, maxX, 1);
        octant |= getOctantHelper(y, minY, maxY, 2);
        octant |= getOctantHelper(z, minZ, maxZ, 4);
        return octant;
    }

    private int getOctantHelper(ComparableValue value, ComparableValue min, ComparableValue max, int bit) {
        ComparableValue mid = getMid(min, max);
        if (value.compare(mid) > 0) {
            return bit;
        } else {
            return 0;
        }
    }

    // Check if the node can be split further
    private void split(ComparableValue minX, ComparableValue maxX, ComparableValue minY, ComparableValue maxY,
            ComparableValue minZ, ComparableValue maxZ) throws IOException {

        // Calculate mid points
        ComparableValue midX = getMid(minX, maxX);
        ComparableValue midY = getMid(minY, maxY);
        ComparableValue midZ = getMid(minZ, maxZ);

        // Create a new array to store the 8 children
        children = new OctreeNode[8];

        // Initialize each child with their respective boundaries
        children[0] = new OctreeNode(minX, midX, minY, midY, minZ, midZ);
        children[1] = new OctreeNode(midX, maxX, minY, midY, minZ, midZ);
        children[2] = new OctreeNode(minX, midX, midY, maxY, minZ, midZ);
        children[3] = new OctreeNode(midX, maxX, midY, maxY, minZ, midZ);
        children[4] = new OctreeNode(minX, midX, minY, midY, midZ, maxZ);
        children[5] = new OctreeNode(midX, maxX, minY, midY, midZ, maxZ);
        children[6] = new OctreeNode(minX, midX, midY, maxY, midZ, maxZ);
        children[7] = new OctreeNode(midX, maxX, midY, maxY, midZ, maxZ);

        // Move existing entries into children
        for (NodeEntry entry : entries) {
            insertIntoChildren(entry.getX(), entry.getY(), entry.getZ(), entry.getPageID());
        }

        entries.clear();
    }

    private ComparableValue getMid(ComparableValue min, ComparableValue max) {
        Object minValue = min.getValue();
        Object maxValue = max.getValue();

        if (minValue instanceof Integer) {
            int mid = ((Integer) minValue + (Integer) maxValue) / 2;
            return new IntegerValue(mid);
        } else if (minValue instanceof Double) {
            double mid = ((Double) minValue + (Double) maxValue) / 2;
            return new DoubleValue(mid);
        } else if (minValue instanceof String) {
            String mid = getMiddleString((String) minValue, (String) maxValue);
            return new StringValue(mid);
        } else if (minValue instanceof Date) {
            long midTimeMillis = (((Date) minValue).getTime() + ((Date) maxValue).getTime()) / 2;
            return new DateValue(new Date(midTimeMillis).toString());
        }
        return null;
    }

    static String getMiddleString(String S, String T) {
        String result = "";

        if (S.length() > T.length()) {
            // concatenate the extra characters of S to the end of T
            int diff = S.length() - T.length();
            T = T + S.substring(S.length() - diff);
        }

        else if (S.length() < T.length()) {
            // concatenate the extra characters of T to the end of S
            int diff = T.length() - S.length();
            S = S + T.substring(T.length() - diff);
        }

        int N = S.length();

        // Stores the base 26 digits after addition
        int[] a1 = new int[N + 1];

        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int) S.charAt(i) - 97
                    + (int) T.charAt(i) - 97;
        }

        // Iterate from right to left
        // and add carry to next position
        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int) a1[i] / 26;
            a1[i] %= 26;
        }

        // Reduce the number to find the middle
        // string by dividing each position by 2
        for (int i = 0; i <= N; i++) {

            // If current value is odd,
            // carry 26 to the next index value
            if ((a1[i] & 1) != 0) {

                if (i + 1 <= N) {
                    a1[i + 1] += 26;
                }
            }

            a1[i] = (int) a1[i] / 2;
        }

        for (int i = 1; i <= N; i++) {
            result += ((char) (a1[i] + 97));
        }

        return result;
    }

    public boolean isLeaf() {
        return children == null;
    }

    private boolean areAllChildrenEmpty() {
        for (OctreeNode child : children) {
            if (!child.entries.isEmpty() || !child.overflowEntries.isEmpty() || child.children != null) {
                return false;
            }
        }
        return true;
    }

    private boolean intersects(ComparableValue minX, ComparableValue maxX, ComparableValue minY, ComparableValue maxY,
            ComparableValue minZ, ComparableValue maxZ) {
        // we need to handle all cases for x,y,z such as (x>2 AND y>2 AND z>2) or (x>2
        // OR y>2 AND z>2) etc
        boolean intersects = true;
        if (minX != null && maxX != null) {
            intersects &= inRange(this.minX, minX, maxX) || inRange(this.maxX, minX, maxX);
        }
        if (minY != null && maxY != null) {
            intersects &= inRange(this.minY, minY, maxY) || inRange(this.maxY, minY, maxY);
        }
        if (minZ != null && maxZ != null) {
            intersects &= inRange(this.minZ, minZ, maxZ) || inRange(this.maxZ, minZ, maxZ);
        }
        return intersects;
    }

    private boolean inRange(ComparableValue value, ComparableValue min, ComparableValue max) {
        // we need to handle all 4 cases (IntegerValue, DoubleValue, StringValue, and
        // DateValue)
        if (value instanceof IntegerValue) {
            return ((IntegerValue) value).getValue() >= ((IntegerValue) min).getValue() &&
                    ((IntegerValue) value).getValue() <= ((IntegerValue) max).getValue();
        } else if (value instanceof DoubleValue) {
            return ((DoubleValue) value).getValue() >= ((DoubleValue) min).getValue() &&
                    ((DoubleValue) value).getValue() <= ((DoubleValue) max).getValue();
        } else if (value instanceof StringValue) {
            return ((StringValue) value).getValue().compareTo(((StringValue) min).getValue()) >= 0 &&
                    ((StringValue) value).getValue().compareTo(((StringValue) max).getValue()) <= 0;
        } else if (value instanceof DateValue) {
            return ((DateValue) value).getValue().compareTo(((DateValue) min).getValue()) >= 0 &&
                    ((DateValue) value).getValue().compareTo(((DateValue) max).getValue()) <= 0;
        }
        return false;
    }

    public List<NodeEntry> getEntries() {
        return entries;
    }

    public List<NodeEntry> getOverflowEntries() {
        return overflowEntries;
    }

    public OctreeNode[] getChildren() {
        return children;
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

    public static int getMaximumEntriesinOctreeNode() {
        return MaximumEntriesinOctreeNode;
    }

}
