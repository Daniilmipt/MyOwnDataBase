import boostbrain.DataBase;

public class Main {
    public static void main(String[] args) throws Exception {
//        Map<String, Object> map1 = new HashMap<>();
//        map1.put("age", 14);
//        map1.put("name", "Daniil");
//
//        Map<String, Object> map2 = new HashMap<>();
//        map2.put("age", 11);
//        map2.put("name", "Maria");
//
//        Map<String, Object> map3 = new HashMap<>();
//        map3.put("age", 19);
//        map3.put("name", "Petya");
//
//        Map<String, Object> map4 = new HashMap<>();
//        map4.put("age", null);
//        map4.put("name", "Ivan");
//        map4.put("surname", "Ivanov");
//
//        Map<String, Object> map5 = new HashMap<>();
//        map5.put("age", null);
//        map5.put("name", null);
//        map5.put("surname", null);
//
//        Map<String, Class<?>> map = new HashMap<>();
//        map.put("age", ((Object) 14).getClass());
//        map.put("name", ((Object) "a").getClass());
//        map.put("surname", ((Object) "a").getClass());
//        main.DataBase db = new main.DataBase(map);
//        db.addRows(List.of(map1, map2, map3, map4));
//        db.addRows(map5, 2);

        DataBase<Object> db1 = DataBase.fromFile("/home/daniil/IdeaProjects/MyOwnDataBase/src/main/java/test1.txt");
        System.out.println(db1.getColumn("Name"));
        System.out.println(db1.getData());
        System.out.println(db1.getJson());

//        DataBase<Object> db2 = DataBase.fromFile("/home/daniil/IdeaProjects/MyOwnDataBase/src/main/java/test1.txt");
//        DataBase<Object> db3 = DataBase.innerJoin(db1, db2, new String[] {"Name"});
//        System.out.println(db3);

//                 "serialization"
//        FileOutputStream fileOutputStream
//                = new FileOutputStream("/home/daniil/IdeaProjects/MyOwnDataBase/src/main/java/test2.txt");
//        ObjectOutputStream objectOutputStream
//                = new ObjectOutputStream(fileOutputStream);
//        objectOutputStream.writeObject(db);
//        objectOutputStream.flush();
//        objectOutputStream.close();

//                "serialization"
//        FileInputStream fileInputStream
//                = new FileInputStream("/home/daniil/IdeaProjects/MyOwnDataBase/src/main/java/test2.txt");
//        ObjectInputStream objectInputStream
//                = new ObjectInputStream(fileInputStream);
//        main.DataBase db1 = (main.DataBase) objectInputStream.readObject();
//        objectInputStream.close();
//        System.out.println(db1.toString());

//        System.out.println(db);
//        System.out.println(db.getColumn("name"));
//        System.out.println(db.getColumn("age"));
//        System.out.println(db.getColumn("surname"));
//        System.out.println(db.getKeys());
    }
}