import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class Cassandra {

    public void createTable(String keyspaceName, String name) {
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect(keyspaceName);
        String query = "CREATE TABLE IF NOT EXISTS " + name + " (id text PRIMARY KEY, "
                + "user_name text, "
                + "tweet_text text, "
                + "country text, "
                + "language text);";
        session.execute(query);
        session.close();
        cluster.close();

        System.out.println("Table created succesfully");
    }

    public void createKeyspace(String name) {
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect();
        String query = "CREATE KEYSPACE IF NOT EXISTS " + name + " WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':1}; ";
        session.execute(query);
        session.execute("USE " + name);
        session.close();
        cluster.close();
        System.out.println("Keyspace created succesfully");
    }

    public void dropTable(String keyspaceName, String tableName) {
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect(keyspaceName);
        String query = "DROP TABLE " + tableName + ";";
        session.execute(query);
        session.close();
        cluster.close();
        System.out.println("Table deleted succesfully");
    }

    public void getAll(String keyspaceName, String tableName) {
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect(keyspaceName);
        String query = "SELECT * FROM " + tableName + ";";
        ResultSet result =  session.execute(query);
        session.close();
        cluster.close();
        System.out.println(result.all());

    }

    public void insertIntoTable(String keyspaceName, String tableName, Tweet values) {
        Cluster cluster = Cluster.builder()
                .addContactPoints("127.0.0.1")
                .build();
        Session session = cluster.connect(keyspaceName);

        Insert insert = QueryBuilder.insertInto("twitter","tweet").
                value("id", String.valueOf(System.currentTimeMillis())).
                value("country", values.userName).
                value("tweet_text", values.place).
                value("user_name", values.text).
                value("language", values.language);

        session.execute(insert.toString());
        session.close();
        cluster.close();

    }

}
