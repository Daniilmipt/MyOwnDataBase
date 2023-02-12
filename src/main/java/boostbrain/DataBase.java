package boostbrain;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import org.json.JSONObject;

public class DataBase<V> implements Serializable {
    private ArrayList<Map<String, V>> data = new ArrayList<>();
    private LinkedHashMap<String, Class<?>> keys;
    private Map<String, LinkedList<V>> columns;

    private LinkedHashMap<String, Class<?>> initializeKeys(Map<String, Class<?>> keys){
        if (!keys.containsKey(null)) {
            return new LinkedHashMap<>(keys);
        }
        else {
            throw new NullPointerException("Key names contain null");
        }
    }
    private Map<String, LinkedList<V>> initializeColumns(){
        Map<String, LinkedList<V>> map = new HashMap<>();
        for(String string: keys.keySet()){
            map.put(string, new LinkedList<V>());
        }
        return map;
    }
    public DataBase(){
        this.keys = new LinkedHashMap<>();
        this.columns = new HashMap<>();
    }
    public DataBase(DataBase<V> db){
        this.keys = db.getKeys();
        this.data = db.getData();
        this.columns = db.getColumns();
    }
    public DataBase(Map<String, Class<?>> keys){
        this.keys = this.initializeKeys(keys);
        this.columns = this.initializeColumns();
    }
    private Map<String, V> updateRow(Map<String, V> row) throws Exception {
        for (String key: this.keys.keySet()){
            if (!row.containsKey(key)){
                row.put(key, null);
            }
            else{
                if (row.get(key)!=null && this.keys.get(key)!=row.get(key).getClass()) {
                    throw new Exception(String.format("Incorrect key \"%s\" type", key));
                }
            }
        }
        return row;
    }
//    private Map<String, V> addNullValues(Map<String, V> row){
//        for (String string: this.keys.keySet()){
//            row.putIfAbsent(string, null);
//        }
//        return row;
//    }

    private void addColumn(Map<String, V> row, Integer i){
        try {
            for (String key: row.keySet()) {
                this.columns.get(key).add(i, row.get(key));
            }
        }
        catch (NullPointerException exc){
            throw new NullPointerException("Index is null");
        }
        this.data.add(i, row);
    }
    private void addColumn(Map<String, V> row){
        for (String key: row.keySet()) {
            this.columns.get(key).add(row.get(key));
        }
        this.data.add(row);
    }

    public void addRows(List<Map<String, V>> rows, Integer i) throws Exception {
        try {
            int k = i;
            for (Map<String, V> row : rows) {
                addRows(row, k++);
            }
        }
        catch (NullPointerException exc){
            throw new NullPointerException("Index is null");
        }
        catch (IndexOutOfBoundsException exc){
            throw new IndexOutOfBoundsException(
                    String.format("Index out of bounds. The table has %d lines", data.size())
            );
        }
    }
    public void addRows(List<Map<String, V>> rows) throws Exception {
        for (Map<String, V> row : rows) {
            addRows(row);
        }
    }
    public void addRows(Map<String, V> row, Integer i) throws Exception {
        try {
            Map<String, V> rowUpdate = this.updateRow(row);
            this.addColumn(rowUpdate, i);
        }
        catch (NullPointerException exc){
            throw new NullPointerException("Index is null");
        }
        catch (IndexOutOfBoundsException exc){
            throw new IndexOutOfBoundsException(
                    String.format("Index out of bounds. The size of table is %d", data.size())
            );
        }
    }
    public void addRows(Map<String, V> row) throws Exception {
        Map<String, V> rowUpdate = this.updateRow(row);
        this.addColumn(rowUpdate);
    }

    public LinkedHashMap<String, Class<?>> getKeys(){
        return keys;
    }
    public Map<String, LinkedList<V>> getColumns(){
        return columns;
    }
    public ArrayList<Map<String, V>> getData(){
        return data;
    }
    public Map<String, V> getRow(Integer i) throws Exception {
        try {
            return data.get(i);
        }
        catch (NullPointerException|IndexOutOfBoundsException exc){
            throw new Exception(exc.getMessage());
        }
    }

    public LinkedList<V> getColumn(String name){
        return columns.get(name);
    }

    private static String[] keyFromFile(Scanner scanner) throws Exception {
        if (scanner.hasNext()){
            return scanner.nextLine().split(" ");
        }
        throw new Exception("Not types in file");
    }
    private static String[] typeFromFile(Scanner scanner) throws Exception {
        if (scanner.hasNext()){
            return scanner.nextLine().split(" ");
        }
        throw new Exception("Not keys in file");
    }
    private static LinkedHashMap<String, Class<?>> joinTypesKeys(String[] keys, String[] types) throws Exception {
        if(keys.length!=types.length){
            throw new Exception("Incorrect file keys and types");
        }
        else {
            LinkedHashMap<String, Class<?>> map = new LinkedHashMap<>();
            for (int i = 0; i < keys.length; i++) {
                if (Objects.equals(types[i], "class.java.lang.String")) {
                    map.put(keys[i], ((Object) "a").getClass());
                } else if (Objects.equals(types[i], "class.java.lang.Float")) {
                    map.put(keys[i], ((Object) 0.0f).getClass());
                } else if (Objects.equals(types[i], "class.java.lang.Double")) {
                    map.put(keys[i], ((Object) 0.0d).getClass());
                } else if (Objects.equals(types[i], "class.java.lang.Byte")) {
                    byte b = 1;
                    map.put(keys[i], ((Object) b).getClass());
                } else if (Objects.equals(types[i], "class.java.lang.Integer")) {
                    map.put(keys[i], ((Object) 2).getClass());
                } else {
                    throw new Exception("Unknowing type");
                }
            }
            return map;
        }
    }
    private Object transformRawData(String string){
        try {
            return Integer.parseInt(string);
        } catch (NumberFormatException exInt) {
            try {
                return Float.parseFloat(string);
            } catch (NumberFormatException exFloat) {
                try {
                    return Double.parseDouble(string);
                } catch (NumberFormatException exDouble) {
                    return string;
                }
            }
        }
    }
    public static DataBase<Object> fromFile(String path) throws Exception {
        File file = new File(path);
        Scanner scanner = new Scanner(file);
        String[] strings;
        String[] type = typeFromFile(scanner);
        String[] key = keyFromFile(scanner);
        Map<String, Object> map;
        DataBase<Object> db = new DataBase<>();

        db.keys = joinTypesKeys(key, type);
        db.columns = db.initializeColumns();
        while (scanner.hasNext()){
            map = new HashMap<>();
            strings = scanner.nextLine().split(" ");
            for (int i = 0; i < key.length; i++){
                map.put(key[i], db.transformRawData(strings[i]));
            }
            db.addRows(map);
        }
        return db;
    }

    private static <T, K> DataBase<Object> initJoinDataBase(DataBase<T> db1, DataBase<K> db2) throws Exception {
        LinkedHashMap<String, Class<?>> db1KeysMap = db1.getKeys();
        LinkedHashMap<String, Class<?>> db2KeysMap = db2.getKeys();
        if(!db1KeysMap.equals(db2KeysMap)){
            throw new Exception("Unequal types of columns");
        }
        LinkedHashMap<String, Class<?>> dbKeys = new LinkedHashMap<>(db1KeysMap);
        dbKeys.putAll(db2KeysMap);
        return new DataBase<>(db2KeysMap);
    }

    public static <T,V> DataBase<Object> innerJoin(DataBase<T> db1, DataBase<V> db2, String[] keys) throws Exception {
        DataBase<Object> db = initJoinDataBase(db1, db2);

        ArrayList<Map<String, T>> data1 = db1.getData();
        ArrayList<Map<String, V>> data2 = db2.getData();
        Map<String, T> row1;
        Map<String, V> row2;
        Map<String, Object> row;
        LinkedList<T> column1;
        LinkedList<V> column2;

        for(String key: keys){
            column1 = db1.getColumn(key);
            column2 = db2.getColumn(key);
            row = new HashMap<>();

            for (int i = 0; i < column1.size(); i++){
                row1 = data1.get(i);
                row.putAll(row1);
                for (int j = 0; j < column2.size(); j++) {
                    row2 = data2.get(j);
                    if (column2.get(j).equals(column1.get(i))) {
                        for (String k: row2.keySet()){
                            row.put(k + "_1", row2.get(k));
                        }
                        db.data.add(row);
                        row = new HashMap<>(row1);
                    }
                }
            }
        }
        return db;
    }

    @Override
    public String toString(){
        StringJoiner mystifying = new StringJoiner("\n");
        Map<String, V> row;
        for(int i = 0; i < data.size(); i++){
            row = data.get(i);
            mystifying.add(i + " " + row.toString());
        }
        return mystifying.toString();
    }

    public JSONObject getJson(){
        ArrayList<Map<String, V>> dt = this.getData();
        Map<Integer, Map<String, V>> map = new HashMap<>();
        for (int i = 0; i < dt.size(); i++){
            map.put(i, dt.get(i));
        }
        return new JSONObject(map);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataBase<?> dataBase = (DataBase<?>) o;
        return Objects.equals(data, dataBase.data);
    }
    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}
