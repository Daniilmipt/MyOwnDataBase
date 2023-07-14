package boostbrain;

import com.fasterxml.jackson.datatype.jdk8.OptionalIntDeserializer;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class DataBase<V> extends AbstractDataBase<V> implements Serializable {
    private List<Map<String, V>> data = new ArrayList<>();
    private LinkedHashMap<String, DataTypes> keys;
    private Map<String, LinkedList<V>> columns;
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
    public DataBase(Map<String, DataTypes> keys){
        this.keys = this.initializeKeys(keys);
        this.columns = this.initializeColumns();
    }

    public static Logger getLogger() {
        return logger;
    }

    private LinkedHashMap<String, DataTypes> initializeKeys(Map<String, DataTypes> keys){
        if (!keys.containsKey(null)) {
            logger.log(Level.FINE,"Initialize keys");
            return new LinkedHashMap<>(keys);
        }
        else {
            throw new NullPointerException("Key names contain null");
        }
    }
    private Map<String, LinkedList<V>> initializeColumns(){
        Map<String, LinkedList<V>> map =
                keys.keySet().stream().collect(Collectors.toMap(x -> x, x -> new LinkedList<>()));
        logger.log(Level.FINE,"Initialize empty columns");
        return map;
    }

    @Override
    public void addRows(List<Map<String, V>> rows, Integer i) throws Exception {
        if(i + rows.size() < data.size()) {
            int k = i;
            for (Map<String, V> row : rows) {
                this.addRows(row, k++);
            }
            logger.log(Level.INFO,"The rows were added");
        }
        else{
            throw new IndexOutOfBoundsException(
                    String.format("Index out of bounds. The table has %d lines", data.size())
            );
        }
    }

    public void addRows(Map<String, V> row, Integer i) throws Exception {
        if(i < data.size()) {
            Map<String, V> rowUpdate = this.updateRow(row);
            logger.log(Level.FINE, "The row_{0} were changed to database format", i);
            this.addToDatabase(rowUpdate, i);
        }
        else{
            throw new IndexOutOfBoundsException(
                    String.format("Index out of bounds. The size of table is %d", data.size())
            );
        }
    }

    private Map<String, V> updateRow(Map<String, V> row) throws Exception {
        Set<String> rowSet = new HashSet<>(this.keys.keySet());
        rowSet.addAll(new HashSet<>(row.keySet()));
        for (String key: rowSet){
            if (!row.containsKey(key)){
                row.put(key, null);
            }
            else if (!this.keys.containsKey(key)){
                throw new Exception("Incorrect row format. Row hase unexpected columns");
            }
            else{
                    if (row.get(key)!=null && !row.get(key).getClass().getName().equals(this.keys.get(key).descr())) {
                    throw new Exception(String.format("Incorrect key \"%s\" type", key));
                }
            }
        }
        return row;
    }

    private void addToDatabase(Map<String, V> row, Integer i) {
        for (String key: row.keySet()) {
            this.columns.get(key).add(i, row.get(key));
        }
        this.data.add(i, row);
    }
    @Override
    public void addRows(List<Map<String, V>> rows) throws Exception {
        for (Map<String, V> row : rows) {
            this.addRows(row);
        }
        logger.log(Level.INFO,"The rows were added");
    }
    public void addRows(Map<String, V> row) throws Exception {
        Map<String, V> rowUpdate = this.updateRow(row);
        this.addToDatabase(rowUpdate);
    }

    private void addToDatabase(Map<String, V> row) {
        for (String key: row.keySet()) {
            this.columns.get(key).add(row.get(key));
        }
        this.data.add(row);
    }

    public LinkedHashMap<String, DataTypes> getKeys(){
        return keys;
    }
    public Map<String, LinkedList<V>> getColumns(){
        return columns;
    }
    public List<Map<String, V>> getData(){
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

    public ArrayList<V> getColumn(String name){
        return new ArrayList<>(columns.get(name));
    }

    public ArrayList<ArrayList<V>> getColumn(String[] names){
        ArrayList<ArrayList<V>> arrayList = new ArrayList<>();
        for(String name: names){
            arrayList.add(getColumn(name));
        }
        return arrayList;
    }

    private static String[] keyFromFile(Scanner scanner) throws Exception {
        if (scanner.hasNext()){
            return scanner.nextLine().replaceAll("\n", "").split(";");
        }
        throw new Exception("Not types in file");
    }
    private static String[] typeFromFile(Scanner scanner) throws Exception {
        if (scanner.hasNext()){
            return scanner.nextLine().replaceAll("\n", "").split(";");
        }
        throw new Exception("Not one key in file");
    }
    private static LinkedHashMap<String, DataTypes> joinTypesKeys(String[] keys, String[] types) throws Exception {
        if(keys.length!=types.length){
            throw new Exception("Incorrect file keys and types");
        }
        else {
            LinkedHashMap<String, DataTypes> map = new LinkedHashMap<>();
            for (int i = 0; i < keys.length; i++) {
                switch (types[i]) {
                    case ("java.lang.String") -> map.put(keys[i], DataTypes.String);
                    case ("java.lang.Float") -> map.put(keys[i], DataTypes.Float);
                    case ("java.lang.Double") -> map.put(keys[i], DataTypes.Double);
                    case ("java.lang.Byte") -> map.put(keys[i], DataTypes.Byte);
                    case ("java.lang.Integer") -> map.put(keys[i], DataTypes.Integer);
                    default -> throw new Exception("Unknowing type: " + types[i]);
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
    public static DataBase<Object> fromFile(String path) throws Exception {
        File file = new File(path);
        Scanner scanner = new Scanner(file);
        DataBase<Object> db = fromFileInit(scanner);
        return fromFileFill(db, scanner);
    }

    private static DataBase<Object> fromFileInit(Scanner scanner) throws Exception {
        String[] type = typeFromFile(scanner);
        String[] key = keyFromFile(scanner);
        DataBase<Object> db = new DataBase<>();

        db.keys = joinTypesKeys(key, type);
        db.columns = db.initializeColumns();
        logger.log(Level.INFO,"Correct initialize database");
        return db;
    }

    private static DataBase<Object> fromFileFill(DataBase<Object> db, Scanner scanner) throws Exception {
        String[] key = new String[db.getKeys().size()];
        db.getKeys().keySet().toArray(key);
        while (scanner.hasNext()){
            Map<String, Object> map = new HashMap<>();
            String[] strings = scanner.nextLine().split(";");
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
    
    public void writeToFile(String path) throws Exception {
        File file = new File(path);
        try(BufferedWriter out = new BufferedWriter(new FileWriter(file))) {

            for (DataTypes type : keys.values()) {
                out.write(type.descr());
                out.write(";");
            }
            out.write("\n");

            for (String nm : keys.keySet()) {
                out.write(nm);
                out.write(";");
            }
            out.write("\n");

            for (Map<String, V> map : data) {
                for (String nm : keys.keySet()) {
                    V value = map.get(nm);
                    String val = value != null ? value.toString() : "";
                    out.write(val);
                    out.write(";");
                }
                out.write("\n");
            }
        }
        catch (IOException ex){
            System.out.println(ex.getMessage());
        }
    }
    

    private static <T, K> DataBase<Object> initJoinDataBase(DataBase<T> db1, DataBase<K> db2, String[] keys) throws Exception {
        Map<String, DataTypes> db1KeysMap = new HashMap<>();
        Map<String, DataTypes> db2KeysMap = new HashMap<>();
        for(String key: keys){
            db1KeysMap.put(key, db1.getKeys().get(key));
            db2KeysMap.put(key, db2.getKeys().get(key));
        }
        if(!db1KeysMap.equals(db2KeysMap)){
            throw new Exception("Unequal types of columns");
        }
        LinkedHashMap<String, DataTypes> dbKeys = new LinkedHashMap<>(db1KeysMap);
        dbKeys.putAll(db2KeysMap);
        logger.log(Level.FINE,"Correct compare databases fields types");
        return new DataBase<>(dbKeys);
    }

    public static <T,V> DataBase<Object> innerJoin(DataBase<T> db1, DataBase<V> db2, String[] keys) throws Exception {
        DataBase<Object> db = initJoinDataBase(db1, db2, keys);
        List<Map<String, T>> data1 = db1.getData();
        List<Map<String, V>> data2 = db2.getData();

        ArrayList<ArrayList<T>> column1 = db1.getColumn(keys);
        ArrayList<ArrayList<V>> column2 = db2.getColumn(keys);
        for (int i = 0; i < data1.size(); i++){
            for (int j = 0; j < data2.size(); j++){
                boolean flag = true;
                Map<String, T> row1 = data1.get(i);
                Map<String, V> row2 = data2.get(j);
                Map<String, Object> row = new HashMap<>(row1);
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
        List<Map<String, V>> dt = this.getData();
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
