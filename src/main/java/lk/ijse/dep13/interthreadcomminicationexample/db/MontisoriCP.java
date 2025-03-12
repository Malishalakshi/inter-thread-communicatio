package lk.ijse.dep13.interthreadcomminicationexample.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;

public class MontisoriCP {
    Scanner sc = new Scanner(System.in);
    private static int DEFAULT_POOL_SIZE;
    private final HashMap<Integer, Connection> MAIN_POOL = new HashMap<>();
    private final HashMap<Integer, Connection> CONSUMER_POOL = new HashMap<>();
    private final int poolSize;

    public MontisoriCP() {
        this(DEFAULT_POOL_SIZE);
    }

    public MontisoriCP(int poolSize) {
        this.poolSize = poolSize;
        try {
            initializePool();
        } catch (IOException | SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public int getPoolSize() {
        return poolSize;
    }

    private void initializePool() throws IOException, SQLException, ClassNotFoundException {
        Properties properties = new Properties();
        properties.load(getClass().getResourceAsStream("/application.properties"));
        System.out.print("Enter pool size: ");
        int poolSize= sc.nextInt();
        if(poolSize<=0){
            String DEFAULT_POOL_SIZE = properties.getProperty( "app.db.DEFAULT_POOL_SIZE" );
            poolSize = Integer.parseInt(  DEFAULT_POOL_SIZE);
        }
        String host = properties.getProperty("app.db.host");
        String port = properties.getProperty("app.db.port");
        String database = properties.getProperty("app.db.database");
        String user = properties.getProperty("app.db.user");
        String password = properties.getProperty("app.db.password");

        Class.forName("com.mysql.cj.jdbc.Driver");

        for (int i = 0; i < poolSize; i++) {
            Connection connection = DriverManager.getConnection("jdbc:mysql://%s:%s/%s"
                    .formatted(host, port, database), user, password);
            MAIN_POOL.put((i + 1) * 10, connection);
        }
    }

    public synchronized ConnectionWrapper getConnection() {
        while (MAIN_POOL.isEmpty()) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        UUID uuid = UUID.randomUUID(); // Generates a random UUID
        Integer key = uuid.hashCode();
        key =  MAIN_POOL.keySet().stream().findFirst().get();
        Connection connection = MAIN_POOL.get(key);
        MAIN_POOL.remove(key);
        CONSUMER_POOL.put(key, connection);
        return new ConnectionWrapper(key, connection);
    }

    public synchronized void releaseConnection(Integer id){
        if (!CONSUMER_POOL.containsKey(id)) throw new RuntimeException("Invalid Connection ID");
        Connection connection = CONSUMER_POOL.get(id);
        CONSUMER_POOL.remove(id);
        MAIN_POOL.put(id, connection);
        notify();
    }

    public synchronized void releaseAllConnections(){
        CONSUMER_POOL.forEach(MAIN_POOL::put);
        CONSUMER_POOL.clear();
        notifyAll();
    }

    public record ConnectionWrapper(Integer id, Connection connection) {
    }
}
