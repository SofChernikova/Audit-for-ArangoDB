import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.DbName;
import com.arangodb.entity.BaseDocument;
import com.arangodb.mapping.ArangoJack;

import kafka.SampleConsumer;
import kafka.SampleProducer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Properties;


public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        ArangoDB arangoDB = new ArangoDB.Builder().user("root").password("1117").serializer(new ArangoJack()).build();

        ArangoDatabase db = arangoDB.db(DbName.of("example_db"));
        ArangoCollection first = db.collection("firstCollection");

        BaseDocument baseDocument = new BaseDocument();
        baseDocument.addAttribute("firstName", "Pavel");
        baseDocument.addAttribute("lastName", "Grishin");
        baseDocument.addAttribute("age", 25);
        baseDocument.addAttribute("profession", "commentator");

        db.collection("firstCollection").insertDocument(baseDocument);

        SampleConsumer sampleConsumer = new SampleConsumer();


    }
}
