package boostbrain;

import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public interface InterfaceDataBase<V> {
    void addRows(List<HashMap<String, V>> rows) throws Exception;
    void addRows(List<HashMap<String, V>> rows, Integer i) throws Exception;
    ArrayList<V> getColumn(String name);
    HashMap<String, V> getRow(Integer i) throws Exception;
    JSONObject getJson();
}
