package main;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements java.io.Serializable {
    private Vector<Hashtable<String, Object>> pageVector;
    private String pageID;

    public Page(String pageID) {
        pageVector = new Vector<Hashtable<String, Object>>();
        this.pageID = pageID;
    }

    public Vector<Hashtable<String, Object>> getPageVector() {
        return pageVector;
    }

    public void setPageVector(Vector<Hashtable<String, Object>> pageVector) {
        this.pageVector = pageVector;
    }

    public Enumeration<Hashtable<String, Object>> getRows() {
        return pageVector.elements();
    }

    public String getPageID() {
        return pageID;
    }

}
