package boostbrain;

import org.springframework.web.bind.annotation.*;


@RestController
public class DataBaseController{
    @RequestMapping(value="/data")
    public String getAllData() throws Exception {
        DataBase<Object> db1 = DataBase.fromFile("/home/daniil/IdeaProjects/MyOwnDataBase/src/main/java/test1.txt");
        return db1.getJson().toString();
    }
}