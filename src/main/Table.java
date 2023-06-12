package main;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements java.io.Serializable {

	private String strTableName;
	private String strClusteringKeyColumn;
	private HashSet<Object> clusteringKeys;
	private Hashtable<String, String> htblColNameType;
	private Hashtable<String, String> htblColNameMin;
	private Hashtable<String, String> htblColNameMax;
	transient private Vector<Page> pagesOfTable;
	private Vector<String> pagesIDS;
	private int pagesCount;
	private int octreesCount;

	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) {
		this.strTableName = strTableName;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		this.htblColNameType = htblColNameType;
		this.htblColNameMin = htblColNameMin;
		this.htblColNameMax = htblColNameMax;
		clusteringKeys = new HashSet<Object>();
		pagesIDS = new Vector<String>();
		pagesOfTable = new Vector<Page>();
		pagesCount = 0;
		octreesCount = 0;

	}

	public String getStrTableName() {
		return strTableName;
	}

	public String getStrClusteringKeyColumn() {
		return strClusteringKeyColumn;
	}

	public Hashtable<String, String> getHtblColNameType() {
		return htblColNameType;
	}

	public Hashtable<String, String> getHtblColNameMin() {
		return htblColNameMin;
	}

	public Hashtable<String, String> getHtblColNameMax() {
		return htblColNameMax;
	}

	public Vector<Page> getPagesOfTable() {
		return pagesOfTable;
	}

	public Vector<String> getPagesIDS() {
		return pagesIDS;
	}

	public HashSet<Object> getClusteringKeys() {
		return clusteringKeys;
	}

	public int getPagesCount() {
		return pagesCount;
	}

	public void setPagesCount(int pagesCount) {
		this.pagesCount = pagesCount;
	}

	private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
		in.defaultReadObject();
		pagesOfTable = new Vector<Page>();
	}

	public int getOctreesCount() {
		return octreesCount;
	}

	public void setOctreesCount(int octreesCount) {
		this.octreesCount = octreesCount;
	}

}