package boostbrain;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface InterfaceDataBase<V> {
    void addRows(List<Map<String, V>> rows) throws Exception;
    void addRows(List<Map<String, V>> rows, Integer i) throws Exception;
    ArrayList<V> getColumn(String name);
    Map<String, V> getRow(Integer i) throws Exception;
    JSONObject getJson();
}
