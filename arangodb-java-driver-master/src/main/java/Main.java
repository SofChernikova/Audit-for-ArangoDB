import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.DbName;
import com.arangodb.entity.BaseDocument;
import com.arangodb.mapping.ArangoJack;
import org.apache.log4j.PropertyConfigurator;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.setProperty("log4j.rootLogger", "WARN");
        PropertyConfigurator.configure(prop);

        ArangoDB arangoDB = new ArangoDB.Builder().user("root").password("1117").serializer(new ArangoJack()).build();

        ArangoDatabase db = arangoDB.db(DbName.of("example_db"));
        ArangoCollection first = db.collection("firstCollection");

        BaseDocument baseDocument = new BaseDocument();
        baseDocument.addAttribute("id", 3);
        baseDocument.addAttribute("name", "Pavel");
        baseDocument.addAttribute("old", 25);

        db.collection("firstCollection").insertDocument(baseDocument);
        db.collection("firstCollection").deleteDocument("224934");
    }
}
