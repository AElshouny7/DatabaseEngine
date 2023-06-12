package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import exceptions.DBAppException;

public class DBApp {
    ObjectOutputStream out;
    ObjectOutputStream in;
    int maximumRowsCountinTablePage;
    int maximumEntriesinOctreeNode;

    public DBApp() throws FileNotFoundException, IOException {
        init();
    }

    // this does whatever initialization you would like
    // or leave it empty if there is no code you want to
    // execute at application startup
    public void init() throws FileNotFoundException, IOException {
        Properties props = new Properties();
        FileInputStream input = new FileInputStream(System.getProperty("user.dir") + "\\\\" + "DBApp.config");
        props.load(input);
        maximumRowsCountinTablePage = Integer.parseInt(props.getProperty("MaximumRowsCountinTablePage"));
    }

    // following method creates one table only
    // strClusteringKeyColumn is the name of the column that will be the primary
    // key and the clustering column as well. The data type of that column will
    // be passed in htblColNameType
    // htblColNameValue will have the column name as key and the data
    // type as value
    // htblColNameMin and htblColNameMax for passing minimum and maximum values
    // for data in the column. Key is the name of the column
    public void createTable(String strTableName, String strClusteringKeyColumn,
            Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
            Hashtable<String, String> htblColNameMax) throws DBAppException, Exception {
        File file = new File(System.getProperty("user.dir") + "\\Data\\"
                + strTableName + ".ser");
        if (file.exists()) {
            throw new Exception("Table name already exists");
        }

        Table table = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin,
                htblColNameMax);
        serializeTable(table);

        try {
            FileWriter metaWriter = new FileWriter(new File("metadata.csv"), true);
            BufferedWriter writer = new BufferedWriter(metaWriter);

            // Add column details to metadata file
            Enumeration<String> colNames = htblColNameType.keys();
            while (colNames.hasMoreElements()) {
                String tableName = strTableName;
                String columnName = colNames.nextElement(); // id , name , gpa
                String columnType = htblColNameType.get(columnName); // java.lang.Integer
                String min = htblColNameMin.get(columnName); // min value
                String max = htblColNameMax.get(columnName); // max value

                String columnDetails = tableName + "," + columnName + "," + columnType + "," + strClusteringKeyColumn
                        + "," + null + "," + null + "," + min + "," + max + "\n";
                writer.write(columnDetails);
            }
            writer.close();
            metaWriter.close();

        } catch (IOException e) {
            throw new IOException("Could not write to metadata file");
        }

    }

    // following method creates an octree
    // depending on the count of column names passed.
    // If three column names are passed,create an octree.
    // If only one or two column names is passed,throw an Exception.
    public void createIndex(String strTableName,
            String[] strarrColName) throws Exception {
        if (strarrColName.length == 1 || strarrColName.length == 2) {
            throw new DBAppException("Invalid number of columns");
        }
        // check if column names are valid
        File file = new File(System.getProperty("user.dir") + File.separator + "Data"
                + File.separator + strTableName + ".ser");
        if (!file.exists()) {
            throw new DBAppException("Table name does not exist");
        }
        Table table = deserializeTable(file);
        for (int i = 0; i < strarrColName.length; i++) {
            if (!table.getHtblColNameType().containsKey(strarrColName[i])) {
                throw new DBAppException("Column name does not exist");
            }
        }

        String minX = null, maxX = null, minY = null, maxY = null, minZ = null, maxZ = null;
        String typeX = "", typeY = "", typeZ = "";

        BufferedReader reader = new BufferedReader(new FileReader("metadata.csv"));
        String line = reader.readLine();
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File("metadata_temp.csv"), true));
        while (line != null) {
            String[] lineArr = line.split(",");
            if (lineArr[0].equals(strTableName)) {

                String colName = lineArr[1];
                String colType = lineArr[2];
                String min = lineArr[6];
                String max = lineArr[7];

                if (colName.equals(strarrColName[0])) {
                    minX = min;
                    maxX = max;
                    typeX = colType;
                    String newLine = "\n" + strTableName + "," + colName + "," + colType + ","
                            + lineArr[3]
                            + "," + strTableName + "_Octree_" + table.getOctreesCount() + ","
                            + "Octree"
                            + "," + min + ","
                            + max;
                    line = newLine;

                } else if (colName.equals(strarrColName[1])) {
                    minY = min;
                    maxY = max;
                    typeY = colType;
                    String newLine = "\n" + strTableName + "," + colName + "," + colType + ","
                            + lineArr[3]
                            + "," + strTableName + "_Octree_" + table.getOctreesCount() + ","
                            + "Octree"
                            + "," + min + ","
                            + max;
                    line = newLine;
                } else if (colName.equals(strarrColName[2])) {
                    minZ = min;
                    maxZ = max;
                    typeZ = colType;
                    String newLine = "\n" + strTableName + "," + colName + "," + colType + ","
                            + lineArr[3]
                            + "," + strTableName + "_Octree_" + table.getOctreesCount() + ","
                            + "Octree"
                            + "," + min + ","
                            + max;
                    line = newLine;
                }
            }
            writer.write(line);
            line = reader.readLine();
        }
        reader.close();
        writer.close();
        // delete old metadata file
        File oldFile = new File("metadata.csv");
        oldFile.delete();
        // rename new metadata file
        File newFile = new File("metadata_temp.csv");
        newFile.renameTo(oldFile);

        // create octree
        Octree octree = new Octree(typeX, typeY, typeZ, minX, maxX, minY, maxY, minZ, maxZ, strarrColName);

        // if table is not empty, loop over all pages
        for (String pageID : table.getPagesIDS()) {
            Page page = deserializePage(new File(System.getProperty("user.dir") + File.separator + "Data"
                    + File.separator + pageID + ".ser"));
            for (Hashtable<String, Object> tuple : page.getPageVector()) {
                Object xcol = tuple.get(strarrColName[0]);
                Object ycol = tuple.get(strarrColName[1]);
                Object zcol = tuple.get(strarrColName[2]);
                octree.insert(xcol, ycol, zcol, pageID);
            }
        }

        serializeOctree(octree, strTableName, table.getOctreesCount());
        table.setOctreesCount(table.getOctreesCount() + 1);
        serializeTable(table);

    }

    private void serializeOctree(Octree octree, String strTableName, int index) {
        try {

            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(
                    System.getProperty("user.dir") + File.separator + "Data"
                            + File.separator + strTableName + "_Octree_" + index + ".ser"));
            out.writeObject(octree);
            out.close();
        } catch (IOException i) {
            i.printStackTrace();
        }

    }

    private Octree deserializeOctree(File file) {
        try {
            ObjectInputStream in = new ObjectInputStream(new FileInputStream(file));
            Octree octree = (Octree) in.readObject();
            in.close();
            return octree;
        } catch (IOException i) {
            i.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out.println("Octree class not found");
            c.printStackTrace();
            return null;
        }
    }

    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public void insertIntoTable(String strTableName,
            Hashtable<String, Object> htblColNameValue)
            throws Exception {

        // get file from data folder and deserialize it to get table object
        File file = getFileFromData(strTableName);
        Table table = deserializeTable(file);
        validationInsert(strTableName, htblColNameValue);

        // search for page to insert in according to clustering key
        Page currentPage = null;
        int pageIndex = 0;
        int insertIndex = 0;

        for (String pageID : table.getPagesIDS()) {
            Page page = deserializePage(new File(System.getProperty("user.dir") + File.separator + "Data"
                    + File.separator + pageID + ".ser"));
            // compare htblColNameValue with min and max values of clustering key

            // if htblColNameValue is less than min, then check previous page has a place
            // insert in it, else insert in the current page
            if (compareValue(htblColNameValue.get(table.getStrClusteringKeyColumn()),
                    page.getPageVector().firstElement().get(table.getStrClusteringKeyColumn())) < 0) {
                if (pageIndex == 0) {
                    currentPage = page;
                    insertIndex = 0;
                    break;
                } else {
                    Page previousPage = deserializePage(new File(
                            System.getProperty("user.dir") + File.separator + "Data"
                                    + File.separator + table.getPagesIDS().get(pageIndex - 1)
                                    + ".ser"));
                    if (previousPage.getPageVector().size() < maximumRowsCountinTablePage) {
                        currentPage = previousPage;
                        pageIndex--;
                        insertIndex = previousPage.getPageVector().indexOf(previousPage.getPageVector().lastElement())
                                + 1;
                        break;
                    }
                }
                // if htblColNameValue is greater than max, then check next page has a place
                // insert in it, else insert in the current page

            } else if (compareValue(htblColNameValue.get(table.getStrClusteringKeyColumn()),
                    page.getPageVector().lastElement().get(table.getStrClusteringKeyColumn())) >= 0) {
                if (pageIndex == table.getPagesIDS().size() - 1) {
                    currentPage = page;
                    insertIndex = page.getPageVector().indexOf(page.getPageVector().lastElement())
                            + 1;
                    break;
                }
                // if htblColNameValue is between min and max, then insert in the current page
            } else if (compareValue(htblColNameValue.get(table.getStrClusteringKeyColumn()),
                    page.getPageVector().firstElement().get(table.getStrClusteringKeyColumn())) > 0
                    && compareValue(htblColNameValue.get(table.getStrClusteringKeyColumn()),
                            page.getPageVector().lastElement().get(table.getStrClusteringKeyColumn())) <= 0) {
                currentPage = page;
                // insertIndex = binarySearchTuples(table.getStrClusteringKeyColumn(),
                // page.getPageVector(),
                // htblColNameValue.get(table.getStrClusteringKeyColumn()), 0,
                // page.getPageVector().size() - 1);

                insertIndex = linerSearchTuples(table.getStrClusteringKeyColumn(), page.getPageVector(),
                        htblColNameValue.get(table.getStrClusteringKeyColumn()));

                break;
            }

            pageIndex++;

        }

        // if page is not found, create new page and insert tuple in it
        if (currentPage == null) {
            Page newPage = new Page(table.getStrTableName() + "_" + table.getPagesCount());
            currentPage = newPage;
            table.setPagesCount(table.getPagesCount() + 1);
            table.getPagesIDS().add(currentPage.getPageID());
            currentPage.getPageVector().add(htblColNameValue);
            table.getClusteringKeys().add(htblColNameValue.get(table.getStrClusteringKeyColumn()));

        } else {
            if (insertIndex < currentPage.getPageVector().size()
                    && compareValue(htblColNameValue.get(table.getStrClusteringKeyColumn()),
                            currentPage.getPageVector().get(insertIndex).get(table.getStrClusteringKeyColumn())) == 0) {
                throw new DBAppException("Primary key already exists");
            }
            currentPage.getPageVector().add(insertIndex, htblColNameValue);
            table.getClusteringKeys().add(htblColNameValue.get(table.getStrClusteringKeyColumn()));
            if (currentPage.getPageVector().size() > maximumRowsCountinTablePage)
                shiftDownTuples(table, currentPage, insertIndex);

        }
        OctreeMethod(strTableName, htblColNameValue, table, currentPage, "insert");
        serializePage(currentPage);
        serializeTable(table);

    }

    private int linerSearchTuples(String strClusteringKeyColumn, Vector<Hashtable<String, Object>> pageVector,
            Object object) throws ParseException {
        for (int i = 0; i < pageVector.size(); i++) {
            if (compareValue(object, pageVector.get(i).get(strClusteringKeyColumn)) < 0)
                return i;
        }
        return pageVector.size();

    }

    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName,
            String strClusteringKeyValue,
            Hashtable<String, Object> htblColNameValue)
            throws Exception { // remove it all and delete then insert

        // get file from data folder and deserialize it to get table object
        File file = getFileFromData(strTableName);
        Table table = deserializeTable(file);
        validationUpdateDelete(strTableName, htblColNameValue);

        if (htblColNameValue.size() > 2)
            for (int i = 0; i < table.getOctreesCount(); i++) {
                Octree octree = deserializeOctree(new File(System.getProperty("user.dir") + "\\Data\\"
                        + table.getStrTableName() + "_Octree_" + i + ".ser"));
                if (htblColNameValue.contains(octree.getStrarrColName()[0])
                        && htblColNameValue.contains(octree.getStrarrColName()[1])
                        && htblColNameValue.contains(octree.getStrarrColName()[2])) {
                    ArrayList<String> pageIDS = octree.exactQuery(htblColNameValue.get(octree.getStrarrColName()[0]),
                            htblColNameValue.get(octree.getStrarrColName()[1]),
                            htblColNameValue.get(octree.getStrarrColName()[2]));
                    for (String pageID : pageIDS) {
                        Page page = deserializePage(new File(System.getProperty("user.dir") + "\\Data\\" + pageID
                                + ".ser"));
                        int tupleIndex = binarySearchTuples(table.getStrClusteringKeyColumn(), page.getPageVector(),
                                strClusteringKeyValue, 0,
                                page.getPageVector().size() - 1);
                        if (tupleIndex != -1) {
                            // update tuples and serialize page
                            page.getPageVector().get(tupleIndex).putAll(htblColNameValue);
                            OctreeMethod(strTableName, htblColNameValue, table, page, "update");
                            serializePage(page);
                        }
                    }
                }
            }

        else {
            // deserialize page and search for tuple
            for (String pageID : table.getPagesIDS()) {
                Page page = deserializePage(new File(System.getProperty("user.dir") + "\\Data\\"
                        + pageID + ".ser"));

                int tupleIndex = binarySearchTuples(table.getStrClusteringKeyColumn(), page.getPageVector(),
                        strClusteringKeyValue, 0,
                        page.getPageVector().size() - 1);
                if (tupleIndex != -1) {
                    // update tuple and serialize page
                    page.getPageVector().get(tupleIndex).putAll(htblColNameValue);
                    serializePage(page);
                    break;
                }
            }
        }

    }

    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue enteries are ANDED together
    public void deleteFromTable(String strTableName,
            Hashtable<String, Object> htblColNameValue)
            throws Exception {

        // get file from data folder and deserialize it to get table object
        File file = getFileFromData(strTableName);
        Table table = deserializeTable(file);
        validationUpdateDelete(strTableName, htblColNameValue);

        if (htblColNameValue.size() > 2)
            for (int i = 0; i < table.getOctreesCount(); i++) {
                Octree octree = deserializeOctree(new File(System.getProperty("user.dir") + "\\Data\\"
                        + table.getStrTableName() + "_Octree_" + i + ".ser"));

                if (htblColNameValue.contains(octree.getStrarrColName()[0])
                        && htblColNameValue.contains(octree.getStrarrColName()[1])
                        && htblColNameValue.contains(octree.getStrarrColName()[2])) {
                    ArrayList<String> pageIDS = octree.exactQuery(htblColNameValue.get(octree.getStrarrColName()[0]),
                            htblColNameValue.get(octree.getStrarrColName()[1]),
                            htblColNameValue.get(octree.getStrarrColName()[2]));
                    for (String pageID : pageIDS) {
                        Page page = deserializePage(new File(System.getProperty("user.dir") + "\\Data\\"
                                + pageID + ".ser"));
                        for (int j = 0; j < page.getPageVector().size(); j++) {
                            Hashtable<String, Object> tuple = page.getPageVector().get(j);
                            boolean isDeleted = true;
                            for (String key : htblColNameValue.keySet()) {
                                if (!tuple.get(key).equals(htblColNameValue.get(key))) {
                                    isDeleted = false;
                                }
                            }
                            if (isDeleted) {
                                page.getPageVector().remove(j);
                                table.getClusteringKeys().remove(j);
                                j--;
                                OctreeMethod(strTableName, htblColNameValue, table, page, "delete");
                            }
                        }
                        serializePage(page);
                    }

                }

            }
        else {
            // deserialize page and search for tuple
            for (int i = 0; i < table.getPagesIDS().size() && i >= 0; i++) {
                String pageID = table.getPagesIDS().get(i);
                Page page = deserializePage(new File(System.getProperty("user.dir") + "\\Data\\"
                        + pageID + ".ser"));

                for (int j = 0; j < page.getPageVector().size() && j >= 0; j++) {
                    Hashtable<String, Object> tuple = page.getPageVector().get(j);
                    boolean isDeleted = true;
                    boolean isDeletedClustering = true;
                    // if clustering key is given in htblColNameValue, then binary search for it and
                    // it else , do a linear search
                    if (htblColNameValue.containsKey(table.getStrClusteringKeyColumn())) {
                        int tupleIndex = binarySearchTuples(table.getStrClusteringKeyColumn(), page.getPageVector(),
                                htblColNameValue.get(table.getStrClusteringKeyColumn()), 0,
                                page.getPageVector().size() - 1);

                        if (tupleIndex != -1) {
                            for (String key : htblColNameValue.keySet()) {
                                if (!tuple.get(key).equals(htblColNameValue.get(key))) {
                                    isDeletedClustering = false;
                                    break;
                                }
                            }
                        }
                    } else {
                        isDeletedClustering = false;
                        for (String key : htblColNameValue.keySet()) {
                            if (!tuple.get(key).equals(htblColNameValue.get(key))) {
                                isDeleted = false;
                                break;
                            }
                        }
                    }
                    if (isDeleted) {
                        page.getPageVector().remove(tuple);
                        // if page is empty, delete it
                        if (page.getPageVector().size() == 0) {
                            table.getPagesIDS().remove(pageID);
                            File pageFile = new File(System.getProperty("user.dir") + "\\Data\\"
                                    + pageID + ".ser");
                            pageFile.delete();
                            j--;
                        } else {
                            serializePage(page);
                        }
                        serializeTable(table);

                        if (isDeletedClustering)
                            return;
                        isDeleted = false;
                        j--;

                    }
                }
            }
        }

    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
            String[] strarrOperators)
            throws Exception {
        // get file from data folder and deserialize it to get table object
        File file = getFileFromData(arrSQLTerms[0].get_strTableName());
        Table table = deserializeTable(file);
        for (SQLTerm arrSQLTerm : arrSQLTerms)
            validationSelect(arrSQLTerm, strarrOperators, table);

        if (arrSQLTerms.length == 3 && strarrOperators.length == 2 && strarrOperators[0] == "AND"
                && strarrOperators[1] == "AND") {
            for (int i = 0; i < table.getOctreesCount(); i++) {
                Octree octree = deserializeOctree(new File(System.getProperty("user.dir") + "\\Data\\"
                        + table.getStrTableName() + "_Octree_" + i + ".ser"));

                SQLTerm x = getCol(arrSQLTerms, octree, 0);
                SQLTerm y = getCol(arrSQLTerms, octree, 1);
                SQLTerm z = getCol(arrSQLTerms, octree, 2);
                if (x != null && y != null && z != null) {
                    if (!(x.get_strOperator().equals("!=") && y.get_strOperator().equals("!=")
                            && z.get_strOperator().equals("!="))) {
                        ArrayList<String> pageIDS = octree.runQuery(x.get_objValue(),
                                y.get_objValue(), z.get_objValue(),
                                x.get_strOperator(), y.get_strOperator(),
                                z.get_strOperator());
                        ArrayList<Hashtable<String, Object>> tuples = new ArrayList<Hashtable<String, Object>>();
                        for (String pageID : pageIDS) {
                            Page page = deserializePage(new File(System.getProperty("user.dir") + "\\Data\\"
                                    + pageID + ".ser"));
                            for (Hashtable<String, Object> tuple : page.getPageVector()) {
                                if (rowSatisfiesConditions(tuple, arrSQLTerms, strarrOperators))
                                    tuples.add(tuple);
                            }

                        }
                        return tuples.iterator();

                    }

                }
            }
        }

        else {
            // scan all pages and search for tuples that satisfy the condition
            ArrayList<Hashtable<String, Object>> tuples = new ArrayList<Hashtable<String, Object>>();
            for (int i = 0; i < table.getPagesIDS().size(); i++) {
                String pageID = table.getPagesIDS().get(i);
                Page page = deserializePage(new File(System.getProperty("user.dir") + "\\Data\\"
                        + pageID + ".ser"));
                for (Hashtable<String, Object> tuple : page.getPageVector()) {
                    if (rowSatisfiesConditions(tuple, arrSQLTerms, strarrOperators))
                        tuples.add(tuple);
                }
            }
            return tuples.iterator();
        }
        return null;

    }

    private SQLTerm getCol(SQLTerm[] arrSQLTerms, Octree octree, int i) {

        for (SQLTerm SQLTerm : arrSQLTerms) {
            if (SQLTerm.get_strColumnName().equals(octree.getStrarrColName()[i]))
                return SQLTerm;
            else if (SQLTerm.get_strColumnName().equals(octree.getStrarrColName()[i]))
                return SQLTerm;
            else if (SQLTerm.get_strColumnName().equals(octree.getStrarrColName()[i]))
                return SQLTerm;
        }
        return null;
    }

    // check if tuple satisfies the condition
    private boolean rowSatisfiesConditions(Hashtable<String, Object> tuple, SQLTerm[] arrSQLTerms,
            String[] strarrOperators) {
        boolean[] results = new boolean[arrSQLTerms.length];
        // Checking if each condition in arrSQLTerms is satisfied or not
        for (int i = 0; i < arrSQLTerms.length; i++) {
            Object columnValue = tuple.get(arrSQLTerms[i].get_strColumnName());
            results[i] = arrSQLTerms[i].checkCondition(columnValue);
        }

        // Now we combine these results using the operators
        boolean finalResult = results[0]; // result for the first condition
        for (int i = 0; i < strarrOperators.length; i++) {
            if ("AND".equalsIgnoreCase(strarrOperators[i])) {
                finalResult = finalResult && results[i + 1];
            } else if ("OR".equalsIgnoreCase(strarrOperators[i])) {
                finalResult = finalResult || results[i + 1];
            } else if ("XOR".equalsIgnoreCase(strarrOperators[i])) {
                finalResult = finalResult ^ results[i + 1];
            }
        }

        return finalResult;
    }

    private void validationSelect(SQLTerm arrSQLTerm, String[] strarrOperators, Table table)
            throws IOException, DBAppException, ParseException {

        boolean colNameExists = false;
        boolean operatorValid = false;
        boolean colTypeValid = false;
        boolean valueValid = false;

        // read from metadata.csv and check if column names exist
        BufferedReader reader = new BufferedReader(new FileReader("metadata.csv"));
        String line = reader.readLine();
        while (line != null) {
            String[] lineArr = line.split(",");
            if (lineArr[0].equals(arrSQLTerm.get_strTableName())) {
                String colName = lineArr[1];
                String colType = lineArr[2];
                String min = lineArr[6];
                String max = lineArr[7];
                // check if column name exists
                if (arrSQLTerm.get_strColumnName().equals(colName)) {
                    colNameExists = true;

                    // check if operator is > or >= or < or <= or != or =
                    if (arrSQLTerm.get_strOperator().equals(">") || arrSQLTerm.get_strOperator().equals(">=")
                            || arrSQLTerm.get_strOperator().equals("<") || arrSQLTerm.get_strOperator().equals("<=")
                            || arrSQLTerm.get_strOperator().equals("!=") || arrSQLTerm.get_strOperator().equals("=")) {
                        operatorValid = true;

                    }
                    // check if value type matches column type
                    if (arrSQLTerm.get_objValue().getClass().getName().equals(colType)) {
                        colTypeValid = true;

                    }
                    // check if value is between min and max
                    if (arrSQLTerm.get_objValue().getClass().getName().equals("java.lang.Integer")) {
                        if ((int) arrSQLTerm.get_objValue() > Integer.parseInt(min)
                                && (int) arrSQLTerm.get_objValue() < Integer.parseInt(max)) {
                            valueValid = true;

                        }
                    } else if (arrSQLTerm.get_objValue().getClass().getName().equals("java.lang.Double")) {
                        if ((double) arrSQLTerm.get_objValue() > Double.parseDouble(min)
                                && (double) arrSQLTerm.get_objValue() < Double.parseDouble(max)) {
                            valueValid = true;

                        }
                    } else if (arrSQLTerm.get_objValue().getClass().getName().equals("java.util.Date")) {
                        if (((Date) arrSQLTerm.get_objValue())
                                .compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(min)) > 0
                                && ((Date) arrSQLTerm.get_objValue())
                                        .compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(max)) < 0) {
                            valueValid = true;

                        }
                    } else if (arrSQLTerm.get_objValue().getClass().getName().equals("java.lang.String")) {
                        if (((String) arrSQLTerm.get_objValue()).compareTo(min) > 0
                                && ((String) arrSQLTerm.get_objValue()).compareTo(max) < 0) {
                            valueValid = true;

                        }
                    }
                }

            }
            line = reader.readLine();
            if (colNameExists && operatorValid && colTypeValid && valueValid)
                break;
        }
        reader.close();

        if (!colNameExists) {
            throw new DBAppException("Column name doesn't exist");
        }
        if (!operatorValid) {
            throw new DBAppException("Operator is not valid");
        }
        if (!colTypeValid) {
            throw new DBAppException("Value type doesn't match column type");
        }
        if (!valueValid) {
            throw new DBAppException("Value is not between min and max");
        }

        // check if operators are correct
        // correct operators are : AND, OR , XOR
        for (int i = 0; i < strarrOperators.length; i++) {
            if (!strarrOperators[i].equals("AND") && !strarrOperators[i].equals("OR")
                    && !strarrOperators[i].equals("XOR")) {
                throw new DBAppException("Operator is not correct");
            }
        }
    }

    private void validationInsert(String strTableName, Hashtable<String, Object> htblColNameValue)
            throws FileNotFoundException, IOException, DBAppException {
        boolean isClusteringKey = false;
        // read from metadata.csv and check if column names exist and check if column
        // types match and validate primary key and validate min and max of the column
        BufferedReader reader = new BufferedReader(new FileReader("metadata.csv"));
        String line = reader.readLine();
        while (line != null) {
            String[] lineArr = line.split(",");
            if (lineArr[0].equals(strTableName)) {
                String colName = lineArr[1];
                String colType = lineArr[2];
                String clusteringKey = lineArr[3];
                String min = lineArr[6];
                String max = lineArr[7];
                if (colName.equals(clusteringKey)) {
                    isClusteringKey = true;
                }
                if (!htblColNameValue.containsKey(colName)) {
                    throw new DBAppException("Column name does not exist");
                }
                String inputType = htblColNameValue.get(colName).getClass().getName();
                if (!inputType.equals(colType)) {
                    throw new DBAppException("Column type does not match");
                }
                if (inputType.equals("java.lang.Integer")) {
                    int input = (int) htblColNameValue.get(colName);
                    int minInt = Integer.parseInt(min);
                    int maxInt = Integer.parseInt(max);
                    if (input < minInt || input > maxInt) {
                        throw new DBAppException("Input value is out of range");
                    }
                } else if (inputType.equals("java.lang.Double")) {
                    double input = (double) htblColNameValue.get(colName);
                    double minDouble = Double.parseDouble(min);
                    double maxDouble = Double.parseDouble(max);
                    if (input < minDouble || input > maxDouble) {
                        throw new DBAppException("Input value is out of range");
                    }
                } else if (inputType.equals("java.util.Date")) {
                    Date input = (Date) htblColNameValue.get(colName);
                    Date minDate = new Date(min);
                    Date maxDate = new Date(max);
                    if (input.before(minDate) || input.after(maxDate)) {
                        throw new DBAppException("Input value is out of range");
                    }
                } else if (inputType.equals("java.lang.String")) {
                    String input = (String) htblColNameValue.get(colName);
                    String minString = min;
                    String maxString = max;
                    if (input.compareTo(minString) < 0 || input.compareTo(maxString) > 0) {
                        throw new DBAppException("Input value is out of range");
                    }
                }
            }
            line = reader.readLine();
        }
        reader.close();
        if (!isClusteringKey) {
            throw new DBAppException("Primary key does not exist");
        }

    }

    private void validationUpdateDelete(String strTableName, Hashtable<String, Object> htblColNameValue)
            throws FileNotFoundException, IOException, DBAppException {

        if (htblColNameValue.isEmpty()) {
            throw new DBAppException("No columns to update or delete");
        }
        boolean isClusteringKey = false;
        // read from metadata.csv and check if column names exist and check if column
        // types match and validate primary key and validate min and max of the column
        BufferedReader reader = new BufferedReader(new FileReader("metadata.csv"));
        String line = reader.readLine();
        while (line != null) {
            String[] lineArr = line.split(",");
            if (lineArr[0].equals(strTableName)) {
                String colName = lineArr[1];
                String colType = lineArr[2];
                String clusteringKey = lineArr[3];
                String min = lineArr[6];
                String max = lineArr[7];
                if (colName.equals(clusteringKey)) {
                    isClusteringKey = true;
                }

                if (htblColNameValue.get(colName) != null) {

                    String inputType = htblColNameValue.get(colName).getClass().getName();
                    if (!inputType.equals(colType)) {
                        throw new DBAppException("Column type does not match");
                    }
                    if (inputType.equals("java.lang.Integer")) {
                        int input = (int) htblColNameValue.get(colName);
                        int minInt = Integer.parseInt(min);
                        int maxInt = Integer.parseInt(max);
                        if (input < minInt || input > maxInt) {
                            throw new DBAppException("Input value is out of range");
                        }
                    } else if (inputType.equals("java.lang.Double")) {
                        double input = (double) htblColNameValue.get(colName);
                        double minDouble = Double.parseDouble(min);
                        double maxDouble = Double.parseDouble(max);
                        if (input < minDouble || input > maxDouble) {
                            throw new DBAppException("Input value is out of range");
                        }
                    } else if (inputType.equals("java.util.Date")) {
                        Date input = (Date) htblColNameValue.get(colName);
                        Date minDate = new Date(min);
                        Date maxDate = new Date(max);
                        if (input.before(minDate) || input.after(maxDate)) {
                            throw new DBAppException("Input value is out of range");
                        }
                    } else if (inputType.equals("java.lang.String")) {
                        String input = (String) htblColNameValue.get(colName);
                        String minString = min;
                        String maxString = max;
                        if (input.compareTo(minString) < 0 || input.compareTo(maxString) > 0) {
                            throw new DBAppException("Input value is out of range");
                        }
                    }
                }
            }
            line = reader.readLine();
        }
        reader.close();
        if (!isClusteringKey) {
            throw new DBAppException("Primary key does not exist");
        }

    }

    private File getFileFromData(String strTableName) throws DBAppException {
        File file = new File(System.getProperty("user.dir") + "\\Data\\"
                + strTableName + ".ser");
        if (!file.exists()) {
            throw new DBAppException("Table does not exist");
        }
        return file;
    }

    private void serializeTable(Table table) {
        try {
            // Creating the ObjectOutputStream and passing it a FileOutputStream
            ObjectOutputStream out = new ObjectOutputStream(
                    new FileOutputStream(System.getProperty("user.dir") + "\\Data\\"
                            + table.getStrTableName() + ".ser"));
            // Writing the object to the output stream
            out.writeObject(table);
            // Closing the output stream
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Table deserializeTable(File file) throws Exception {
        FileInputStream fileIn = new FileInputStream(file);
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Table table = (Table) in.readObject();
        in.close();
        fileIn.close();
        return table;
    }

    private void serializePage(Page page) {
        try {
            // Creating the ObjectOutputStream and passing it a FileOutputStream
            ObjectOutputStream out = new ObjectOutputStream(
                    new FileOutputStream(System.getProperty("user.dir") + "\\Data\\"
                            + page.getPageID() + ".ser"));
            // Writing the object to the output stream
            out.writeObject(page);
            // Closing the output stream
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Page deserializePage(File file) throws Exception {
        FileInputStream fileIn = new FileInputStream(file);
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Page page = (Page) in.readObject();
        in.close();
        fileIn.close();
        return page;
    }

    private int compareValue(Object object, Object object2) throws ParseException {
        String type = object.getClass().getName();
        if (type.equals("java.lang.Integer")) {
            int value1 = (int) object;
            int value2 = 0;
            if (object2.getClass().getName().equals("java.lang.String")) {
                value2 = (int) Integer.parseInt((String) object2);
            } else {
                value2 = (int) object2;
            }

            return Integer.compare(value1, value2);

        } else if (type.equals("java.lang.Double")) {
            double value1 = (double) object;
            double value2 = 0.0;
            if (object2.getClass().getName().equals("java.lang.String")) {
                value2 = (Double) Double.parseDouble((String) object2);
            } else {
                value2 = (double) object2;
            }
            return Double.compare(value1, value2);
        } else if (type.equals("java.util.Date")) {
            Date value1 = (Date) object;
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Date value2 = null;
            if (object2.getClass().getName().equals("java.lang.String")) {
                value2 = (Date) format.parse((String) object2);
            } else {
                value2 = (Date) object2;
            }
            return value1.compareTo(value2);
        } else if (type.equals("java.lang.String")) {
            String value1 = (String) object;
            String value2 = (String) object2;
            return value1.compareTo(value2);
        }
        return 0;
    }

    private int binarySearchTuples(String StrClusteringKeyColumn, Vector<Hashtable<String, Object>> pageVector,
            Object object, int i, int j) throws ParseException {
        if (i == j)
            return i;
        if (j < i)
            return -1;
        int mid = (int) Math.ceil((((double) i) + j) / 2);
        int compare = compareValue(pageVector.get(mid).get(StrClusteringKeyColumn), object);
        if (compare == 0) {
            return mid;
        } else if (compare > 0) {
            return binarySearchTuples(StrClusteringKeyColumn, pageVector, object, i, mid - 1);
        } else {
            return binarySearchTuples(StrClusteringKeyColumn, pageVector, object, mid + 1, j);
        }
    }

    private void shiftDownTuples(Table table, Page currentPage, int insertIndex) throws IOException {
        // check if the current page is full
        if (currentPage.getPageVector().size() > maximumRowsCountinTablePage) {
            // check if there are any more pages in the table
            if (table.getPagesIDS().indexOf(currentPage.getPageID()) < table.getPagesIDS().size() - 1) {
                // if there are more pages, shift tuples to the next page
                Page nextPage = null;
                try {
                    nextPage = deserializePage(new File(System.getProperty("user.dir") + "\\Data\\"
                            + table.getPagesIDS().get(table.getPagesIDS().indexOf(currentPage.getPageID()) + 1)
                            + ".ser"));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                Hashtable<String, Object> lastTuple = currentPage.getPageVector().lastElement();
                currentPage.getPageVector().remove(lastTuple);
                nextPage.getPageVector().add(0, lastTuple);
                OctreeMethod(table.getStrTableName(), lastTuple, table, nextPage, "update");
                serializePage(currentPage);
                serializePage(nextPage);
                // recursively shift down tuples in the next page
                shiftDownTuples(table, nextPage, 0);
            } else {
                // if there are no more pages, create a new page and insert the last tuple
                Page newPage = new Page(table.getStrTableName() + "_" + table.getPagesCount());
                table.setPagesCount(table.getPagesCount() + 1);
                table.getPagesIDS().add(newPage.getPageID());
                Hashtable<String, Object> lastTuple = currentPage.getPageVector().lastElement();
                currentPage.getPageVector().removeElementAt(currentPage.getPageVector().size() - 1);
                OctreeMethod(table.getStrTableName(), lastTuple, table, newPage, "update");
                newPage.getPageVector().add(0, lastTuple);
                serializePage(currentPage);
                serializePage(newPage);

            }

        }

    }

    private void OctreeMethod(String strTableName, Hashtable<String, Object> htblColNameValue, Table table,
            Page page, String method) throws IOException {
        // insert in octree
        if (table.getOctreesCount() != 0) {
            for (int i = 0; i < table.getOctreesCount(); i++) {
                Octree octree = deserializeOctree(new File(System.getProperty("user.dir") + File.separator + "Data"
                        + File.separator + table.getStrTableName() + "_Octree_" + i + ".ser"));
                if (method.equals("insert"))
                    octree.insert(htblColNameValue.get(octree.getStrarrColName()[0]),
                            htblColNameValue.get(octree.getStrarrColName()[1]),
                            htblColNameValue.get(octree.getStrarrColName()[2]), page.getPageID());
                else if (method.equals("delete"))
                    octree.delete(htblColNameValue.get(octree.getStrarrColName()[0]),
                            htblColNameValue.get(octree.getStrarrColName()[1]),
                            htblColNameValue.get(octree.getStrarrColName()[2]));
                else if (method.equals("update")) {
                    octree.update(htblColNameValue.get(octree.getStrarrColName()[0]),
                            htblColNameValue.get(octree.getStrarrColName()[1]),
                            htblColNameValue.get(octree.getStrarrColName()[2]), page.getPageID());
                }

                serializeOctree(octree, strTableName, i);
            }
        }
    }

    private static void insertCoursesRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader coursesTable = new BufferedReader(new FileReader("courses_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = coursesTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            int year = Integer.parseInt(fields[0].trim().substring(0, 4));
            int month = Integer.parseInt(fields[0].trim().substring(5, 7));
            int day = Integer.parseInt(fields[0].trim().substring(8));

            Date dateAdded = new Date(year - 1900, month - 1, day);

            row.put("date_added", fields[0].trim());

            row.put("course_id", fields[1]);
            row.put("course_name", fields[2]);
            row.put("hours", Integer.parseInt(fields[3]));
            row.put("date_added", dateAdded);

            dbApp.insertIntoTable("courses", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        coursesTable.close();
    }

    private static void insertStudentRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader studentsTable = new BufferedReader(new FileReader("students_table.csv"));
        String record;
        int c = limit;
        if (limit == -1) {
            c = 1;
        }

        Hashtable<String, Object> row = new Hashtable<>();
        while ((record = studentsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("id", fields[0]);
            row.put("first_name", fields[1]);
            row.put("last_name", fields[2]);

            int year = Integer.parseInt(fields[3].trim().substring(0, 4));
            int month = Integer.parseInt(fields[3].trim().substring(5, 7));
            int day = Integer.parseInt(fields[3].trim().substring(8));

            Date dob = new Date(year - 1900, month - 1, day);
            row.put("dob", dob);

            double gpa = Double.parseDouble(fields[4].trim());

            row.put("gpa", gpa);

            dbApp.insertIntoTable("students", row);
            row.clear();
            if (limit != -1) {
                c--;
            }
        }
        studentsTable.close();
    }

    private static void insertTranscriptsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader transcriptsTable = new BufferedReader(new FileReader("transcripts_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = transcriptsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("gpa", Double.parseDouble(fields[0].trim()));
            row.put("student_id", fields[1].trim());
            row.put("course_name", fields[2].trim());

            String date = fields[3].trim();
            int year = Integer.parseInt(date.substring(0, 4));
            int month = Integer.parseInt(date.substring(5, 7));
            int day = Integer.parseInt(date.substring(8));

            Date dateUsed = new Date(year - 1900, month - 1, day);
            row.put("date_passed", dateUsed);

            dbApp.insertIntoTable("transcripts", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        transcriptsTable.close();
    }

    private static void insertPCsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader pcsTable = new BufferedReader(new FileReader("pcs_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = pcsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("pc_id", Integer.parseInt(fields[0].trim()));
            row.put("student_id", fields[1].trim());

            dbApp.insertIntoTable("pcs", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        pcsTable.close();
    }

    private static void createTranscriptsTable(DBApp dbApp) throws Exception {
        // Double CK
        String tableName = "transcripts";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("gpa", "java.lang.Double");
        htblColNameType.put("student_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("date_passed", "java.util.Date");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("gpa", "0.7");
        minValues.put("student_id", "43-0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("date_passed", "1990-01-01");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("gpa", "5.0");
        maxValues.put("student_id", "99-9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("date_passed", "2020-12-31");

        dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
    }

    private static void createStudentTable(DBApp dbApp) throws Exception {
        // String CK
        String tableName = "students";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("first_name", "java.lang.String");
        htblColNameType.put("last_name", "java.lang.String");
        htblColNameType.put("dob", "java.util.Date");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("id", "43-0000");
        minValues.put("first_name", "AAAAAA");
        minValues.put("last_name", "AAAAAA");
        minValues.put("dob", "1990-01-01");
        minValues.put("gpa", "0.7");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("id", "99-9999");
        maxValues.put("first_name", "zzzzzz");
        maxValues.put("last_name", "zzzzzz");
        maxValues.put("dob", "2000-12-31");
        maxValues.put("gpa", "5.0");

        dbApp.createTable(tableName, "id", htblColNameType, minValues, maxValues);
    }

    private static void createPCsTable(DBApp dbApp) throws Exception {
        // Integer CK
        String tableName = "pcs";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("pc_id", "java.lang.Integer");
        htblColNameType.put("student_id", "java.lang.String");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("pc_id", "0");
        minValues.put("student_id", "43-0000");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("pc_id", "20000");
        maxValues.put("student_id", "99-9999");

        dbApp.createTable(tableName, "pc_id", htblColNameType, minValues, maxValues);
    }

    private static void createCoursesTable(DBApp dbApp) throws Exception {
        // Date CK
        String tableName = "courses";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("date_added", "java.util.Date");
        htblColNameType.put("course_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("hours", "java.lang.Integer");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("date_added", "1901-01-01");
        minValues.put("course_id", "0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("hours", "1");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("date_added", "2020-12-31");
        maxValues.put("course_id", "9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("hours", "24");

        dbApp.createTable(tableName, "date_added", htblColNameType, minValues, maxValues);

    }

    public static void printTable(DBApp dbApp, String tableName) throws Exception {
        Table table = dbApp.deserializeTable(new File(tableName + ".ser"));

        System.out.println("Pages:");

        for (int i = 0; i < table.getPagesIDS().size(); i++) {
            Page page = dbApp.deserializePage(new File(tableName + "_" + table.getPagesIDS().get(i) + ".ser"));

            for (int j = 0; j < page.getPageVector().size(); j++) {
                Hashtable<String, Object> record = page.getPageVector().get(j);
                System.out.println("Record " + j + ":");
                for (String colName : record.keySet()) {
                    System.out.print(colName + ": " + record.get(colName));
                }
            }

            System.out.println("-----------------------");

        }
    }

    public static void main(String[] args) throws Exception {
        DBApp db = new DBApp();
        db.init();
        createCoursesTable(db);
        createPCsTable(db);
        createTranscriptsTable(db);
        createStudentTable(db);
        insertPCsRecords(db, 500);
        insertTranscriptsRecords(db, 500);
        insertStudentRecords(db, 500);
        insertCoursesRecords(db, 500);
        System.out.println("done");
    }

}