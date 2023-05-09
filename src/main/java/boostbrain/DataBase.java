package boostbrain;

import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataBase<V> extends AbstractDataBase<V> implements Serializable {
    private ArrayList<HashMap<String, V>> data = new ArrayList<>();
    private LinkedHashMap<String, Class<?>> keys;
    private HashMap<String, LinkedList<V>> columns;
    private final static Logger logger = Logger.getLogger(DataBase.class.getName());

    public DataBase(){
        this.keys = new LinkedHashMap<>();
        this.columns = new HashMap<>();
    }
    public DataBase(DataBase<V> db){
        this.keys = db.getKeys();
        this.data = db.getData();
        this.columns = db.getColumns();
    }
    public DataBase(HashMap<String, Class<?>> keys){
        this.keys = this.initializeKeys(keys);
        this.columns = this.initializeColumns();
    }

    private LinkedHashMap<String, Class<?>> initializeKeys(@NotNull HashMap<String, Class<?>> keys){
        if (!keys.containsKey(null)) {
            logger.log(Level.FINE,"Initialize keys");
            return new LinkedHashMap<>(keys);
        }
        else {
            throw new NullPointerException("Key names contain null");
        }
    }
    private HashMap<String, LinkedList<V>> initializeColumns(){
        HashMap<String, LinkedList<V>> map = new HashMap<>();
        for(String string: keys.keySet()){
            map.put(string, new LinkedList<>());
        }
        logger.log(Level.FINE,"Initialize empty columns");
        return map;
    }

    @Override
    public void addRows(@NotNull List<HashMap<String, V>> rows, @NotNull Integer i) throws Exception {
        if(i + rows.size() < data.size()) {
            int k = i;
            for (HashMap<String, V> row : rows) {
                addRows(row, k++);
            }
            logger.log(Level.INFO,"The rows were added");
        }
        else{
            throw new IndexOutOfBoundsException(
                    String.format("Index out of bounds. The table has %d lines", data.size())
            );
        }
    }

    public void addRows(@NotNull HashMap<String, V> row,@NotNull Integer i) throws Exception {
        if(i < data.size()) {
            HashMap<String, V> rowUpdate = this.updateRow(row);
            logger.log(Level.FINE, "The row_{0} were changed to database format", i);
            this.addToDatabase(rowUpdate, i);
        }
        else{
            throw new IndexOutOfBoundsException(
                    String.format("Index out of bounds. The size of table is %d", data.size())
            );
        }
    }

    private HashMap<String, V> updateRow(@NotNull HashMap<String, V> row) throws Exception {
        HashSet<String> rowSet = new HashSet<>(this.keys.keySet());
        rowSet.addAll(new HashSet<>(row.keySet()));
        for (String key: rowSet){
            if (!row.containsKey(key)){
                row.put(key, null);
            }
            else if (!this.keys.containsKey(key)){
                throw new Exception("Incorrect row format. Row hase unexpected columns");
            }
            else{
                if (row.get(key)!=null && this.keys.get(key)!=row.get(key).getClass()) {
                    throw new Exception(String.format("Incorrect key \"%s\" type", key));
                }
            }
        }
        return row;
    }

    private void addToDatabase(@NotNull HashMap<String, V> row, @NotNull Integer i) {
        for (String key: row.keySet()) {
            this.columns.get(key).add(i, row.get(key));
        }
        this.data.add(i, row);
    }
    @Override
    public void addRows(@NotNull List<HashMap<String, V>> rows) throws Exception {
        for (HashMap<String, V> row : rows) {
            addRows(row);
        }
        logger.log(Level.INFO,"The rows were added");
    }
    public void addRows(@NotNull HashMap<String, V> row) throws Exception {
        HashMap<String, V> rowUpdate = this.updateRow(row);
        this.addToDatabase(rowUpdate);
    }

    private void addToDatabase(@NotNull HashMap<String, V> row) {
        for (String key: row.keySet()) {
            this.columns.get(key).add(row.get(key));
        }
        this.data.add(row);
    }

    public LinkedHashMap<String, Class<?>> getKeys(){
        return keys;
    }
    public HashMap<String, LinkedList<V>> getColumns(){
        return columns;
    }
    public ArrayList<HashMap<String, V>> getData(){
        return data;
    }
    public HashMap<String, V> getRow(@NotNull Integer i) throws Exception {
        try {
            return data.get(i);
        }
        catch (NullPointerException|IndexOutOfBoundsException exc){
            throw new Exception(exc.getMessage());
        }
    }

    public ArrayList<V> getColumn(@NotNull String name){
        return new ArrayList<>(columns.get(name));
    }

    public ArrayList<ArrayList<V>> getColumn(@NotNull String[] names){
        ArrayList<ArrayList<V>> arrayList = new ArrayList<>();
        for(String name: names){
            arrayList.add(getColumn(name));
        }
        return arrayList;
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
                switch (types[i]) {
                    case ("class.java.lang.String") -> map.put(keys[i], ((Object) "a").getClass());
                    case ("class.java.lang.Float") -> map.put(keys[i], ((Object) 0.0f).getClass());
                    case ("class.java.lang.Double") -> map.put(keys[i], ((Object) 0.0d).getClass());
                    case ("class.java.lang.Byte") -> map.put(keys[i], ((Object) (byte) 1).getClass());
                    case ("class.java.lang.Integer") -> map.put(keys[i], ((Object) 2).getClass());
                    default -> throw new Exception("Unknowing type");
                }
            }
            logger.log(Level.INFO,"The keys and their types were joined");
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
    public static @NotNull DataBase<Object> fromFile(String path) throws Exception {
        File file = new File(path);
        Scanner scanner = new Scanner(file);
        DataBase<Object> db = fromFileInit(scanner);
        return fromFileFill(db, scanner);
    }

    private static @NotNull DataBase<Object> fromFileInit(Scanner scanner) throws Exception {
        String[] type = typeFromFile(scanner);
        String[] key = keyFromFile(scanner);
        DataBase<Object> db = new DataBase<>();

        db.keys = joinTypesKeys(key, type);
        db.columns = db.initializeColumns();
        logger.log(Level.INFO,"Correct initialize database");
        return db;
    }

    private static @NotNull DataBase<Object> fromFileFill(DataBase<Object> db, Scanner scanner) throws Exception {
        String[] key = new String[db.getKeys().size()];
        db.getKeys().keySet().toArray(key);
        while (scanner.hasNext()){
            HashMap<String, Object> map = new HashMap<>();
            String[] strings = scanner.nextLine().split(" ");
            for (int i = 0; i < key.length; i++){
                if(i >= strings.length){
                    map.put(key[i], null);
                }
                else {
                    map.put(key[i], db.transformRawData(strings[i]));
                }
            }
            db.addRows(map);
        }
        logger.log(Level.INFO,"Correct fill database");
        return db;
    }

    private static <T, K> DataBase<Object> initJoinDataBase(DataBase<T> db1, DataBase<K> db2, String[] keys) throws Exception {
        HashMap<String, Class<?>> db1KeysMap = new HashMap<>();
        HashMap<String, Class<?>> db2KeysMap = new HashMap<>();
        for(String key: keys){
            db1KeysMap.put(key, db1.getKeys().get(key));
            db2KeysMap.put(key, db2.getKeys().get(key));
        }
        if(!db1KeysMap.equals(db2KeysMap)){
            throw new Exception("Unequal types of columns");
        }
        LinkedHashMap<String, Class<?>> dbKeys = new LinkedHashMap<>(db1KeysMap);
        dbKeys.putAll(db2KeysMap);
        logger.log(Level.FINE,"Correct compare databases fields types");
        return new DataBase<>(dbKeys);
    }

    public static <T,V> DataBase<Object> innerJoin(DataBase<T> db1, DataBase<V> db2, String[] keys) throws Exception {
        DataBase<Object> db = initJoinDataBase(db1, db2, keys);
        ArrayList<HashMap<String, T>> data1 = db1.getData();
        ArrayList<HashMap<String, V>> data2 = db2.getData();

        ArrayList<ArrayList<T>> column1 = db1.getColumn(keys);
        ArrayList<ArrayList<V>> column2 = db2.getColumn(keys);
        for (int i = 0; i < data1.size(); i++){
            for (int j = 0; j < data2.size(); j++){
                boolean flag = true;
                HashMap<String, T> row1 = data1.get(i);
                HashMap<String, V> row2 = data2.get(j);
                HashMap<String, Object> row = new HashMap<>(row1);
                for(int k = 0; k < column1.size(); k++){
                    if (column1.get(k).get(i) == null || column2.get(k).get(j) == null ||
                            column1.get(k).get(i) != column1.get(k).get(j)) {
                        flag = false;
                        break;
                    }
                }
                if(flag) {
                    for (String ks : row2.keySet()) {
                        row.put(ks + "_1", row2.get(ks));
                    }
                    db.data.add(row);
                }
            }
        }
        logger.log(Level.INFO,"Valid inner join");
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
        ArrayList<HashMap<String, V>> dt = this.getData();
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
