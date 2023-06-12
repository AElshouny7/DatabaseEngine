package main;

import java.util.Objects;

public class NodeEntry implements java.io.Serializable {
    private final ComparableValue x, y, z;
    private final String pageID;

    public NodeEntry(ComparableValue x, ComparableValue y, ComparableValue z, String pageID) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.pageID = pageID;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NodeEntry nodeEntry = (NodeEntry) obj;
        return Objects.equals(x.getValue(), nodeEntry.x.getValue()) &&
                Objects.equals(y.getValue(), nodeEntry.y.getValue()) &&
                Objects.equals(z.getValue(), nodeEntry.z.getValue()) &&
                Objects.equals(pageID, nodeEntry.pageID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y, z, pageID);
    }

    public ComparableValue getX() {
        return x;
    }

    public ComparableValue getY() {
        return y;
    }

    public ComparableValue getZ() {
        return z;
    }

    public String getPageID() {
        return pageID;
    }

    
}