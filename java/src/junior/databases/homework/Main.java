package junior.databases.homework;

import junior.databases.homework.*;
import java.sql.*;

public class Main {
    private static Connection connection = null;

    private static void initDatabase() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");

        connection = DriverManager.getConnection(
                "jdbc:postgresql://localhost/orm", "postgres",
                "password");
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        initDatabase();

        Entity.setDatabase(connection);



    }
}
