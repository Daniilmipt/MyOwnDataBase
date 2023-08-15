import boostbrain.DataBase;
import boostbrain.DataTypes;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Main {
    public static void main(String[] args) throws Exception {
        HashMap<String, Object> map1 = new HashMap<>();
        map1.put("age", 14);
        map1.put("name", "Daniil");

        HashMap<String, Object> map2 = new HashMap<>();
        map2.put("age", 11);
        map2.put("name", "Maria");

        HashMap<String, Object> map3 = new HashMap<>();
        map3.put("age", 19);
        map3.put("name", "Petya");

        HashMap<String, Object> map4 = new HashMap<>();
        map4.put("age", null);
        map4.put("name", "Ivan");
        map4.put("surname", "Ivanov");

        HashMap<String, Object> map5 = new HashMap<>();
        map5.put("age", null);
        map5.put("name", null);
        map5.put("surname", null);

        HashMap<String, DataTypes> map = new HashMap<>();
        map.put("age", DataTypes.Integer);
        map.put("name", DataTypes.String);
        map.put("surname", DataTypes.String);
        DataBase db = new DataBase(map);
        db.addRows(List.of(map1, map2, map3, map4));
        db.addRows(map5, 2);

        System.out.println(db);
        System.out.println();
        map1 = new HashMap<>();
        map1.put("age", 14);
        map1.put("name", "Daniil");

        map2 = new HashMap<>();
        map2.put("age", 11);
        map2.put("name", "Maria");


        HashMap<String, DataTypes> map_x = new HashMap<>();
        map_x.put("age", DataTypes.Integer);
        map_x.put("name", DataTypes.String);
        DataBase db1 = new DataBase(map_x);
        db1.addRows(List.of(map1, map2));
        System.out.println(db1);
        System.out.println();

        String[] str = {"name", "age"};
        DataBase db2 = DataBase.innerJoin(db, db1, str);

        System.out.println(db2);

//        db.writeToFile("/home/daniil/MyOwnDataBase/src/main/java/test3.txt");


    }
}