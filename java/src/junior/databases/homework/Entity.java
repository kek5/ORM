package junior.databases.homework;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.sql.*;
import java.lang.reflect.Constructor;
import java.util.Date;

public abstract class Entity {
    private static String DELETE_QUERY   = "DELETE FROM \"%1$s\" WHERE %1$s_id=?";
    private static String INSERT_QUERY   = "INSERT INTO \"%1$s\" (%2$s) VALUES (%3$s) RETURNING %1$s_id";
    private static String LIST_QUERY     = "SELECT * FROM \"%s\"";
    private static String SELECT_QUERY   = "SELECT * FROM \"%1$s\" WHERE %1$s_id=?";
    private static String CHILDREN_QUERY = "SELECT * FROM \"%1$s\" WHERE %2$s_id=?";
    private static String SIBLINGS_QUERY = "SELECT * FROM \"%1$s\" NATURAL JOIN \"%2$s\" WHERE %3$s_id=?";
    private static String UPDATE_QUERY   = "UPDATE \"%1$s\" SET %2$s WHERE %1$s_id=?";

    private static Connection db = null;

    private Map<String, Object> fields = new HashMap<>();
    private boolean isLoaded = false;
    private boolean isModified = true;
    private String table = null;
    private int id = 0;


    public Entity() {
        this.setTableName(this.getClass().getSimpleName().toLowerCase());
    }

    public Entity(Integer id) {
        this();
        if(id < 1) {
            System.out.println("Stupid... Put another ID!");
            return;
        }
        this.id = id;
        this.load();
    }

    // my methods
    public void setTableName(String tableName) {
        this.table = tableName;
    }
    public void printMap() {
        String key = "";
        for (int i = 0; i < this.fields.size(); i++) {
            key = this.fields.keySet().toArray()[i].toString();
            System.out.print(key + "+++");
            System.out.println(this.fields.get(key));
        }
    }
    // the end

    private void load() {
        String statement = String.format(Entity.SELECT_QUERY, this.table);
        try {
            PreparedStatement query = Entity.db.prepareStatement(statement);
            query.setInt(1, this.id);
            ResultSet result = query.executeQuery();
            ResultSetMetaData data = result.getMetaData();
            int size = data.getColumnCount() + 1;
            result.next();

            for (int i = 1; i < size; i++) {
                this.fields.put(data.getColumnName(i), result.getObject(i));
            }
            this.isLoaded = true;
        } catch (SQLException e) {
            System.out.println("Entity with such ID was NOT found.");
            this.id = 0;
            e.printStackTrace();
        }
    }

    public static final void setDatabase(Connection connection) {
        if(connection != null) {
            Entity.db = connection;
        } else {
            System.out.println("Bad Connection!");
        }
    }
    public final int getId() {
        return this.id;
    }
    public final java.util.Date getCreated() {
        String dateString = null;
        java.util.Date date = null;
        SimpleDateFormat formatter = new SimpleDateFormat("EEEE, MMMM d,yyyy h:mm,a", Locale.ENGLISH);
        formatter.setTimeZone(TimeZone.getTimeZone("GMT+2"));
        String statement = "select " + this.table + "_created from " + "\"" + this.table + "\"" + " WHERE " + this.table + "_id =" + this.id;
        try {
            PreparedStatement ps = Entity.db.prepareStatement(statement);
            ResultSet result = ps.executeQuery();
            result.next();
            Long time = (long)result.getInt(1) * 1000;
            date = new Date(time);
            dateString = formatter.format(date);
            try{
                date = formatter.parse(dateString);
            } catch(ParseException e) {
                e.printStackTrace();
            }
        } catch(SQLException e) {
            e.printStackTrace();
        }

        return date;
    }
    public final java.util.Date getUpdated() {
        String dateString = null;
        java.util.Date date = null;
        SimpleDateFormat formatter = new SimpleDateFormat("EEEE, MMMM d,yyyy h:mm,a", Locale.ENGLISH);
        formatter.setTimeZone(TimeZone.getTimeZone("GMT+2"));
        String statement = "select " + this.table + "_updated from " + "\"" + this.table + "\"" + " WHERE " + this.table + "_id =" + this.id;
        try {
            PreparedStatement ps = Entity.db.prepareStatement(statement);
            ResultSet result = ps.executeQuery();
            result.next();
            Long time = (long)result.getInt(1) * 1000;
            date = new Date(time);
            dateString = formatter.format(date);
            try{
                date = formatter.parse(dateString);
            } catch(ParseException e) {
                e.printStackTrace();
            }
        } catch(SQLException e) {
            e.printStackTrace();
        }

        return date;
    }

    public final Object getColumn(String name) {
        try{
            if(this.isLoaded) {
                if(this.isModified) {
                    this.save();
                }
                this.load();
            }
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return this.fields.get(name);
    }
    public final void setColumn(String name, Object value) {
        this.fields.put(this.table + "_" + name, value);
        this.isModified = true;
    }
//    public final <T extends Entity> T getParent(Class<T> cls) {
        // get parent id from fields as <classname>_id, create and return an instance of class T with that id
//    }
//
//    public final <T extends Entity> List<T> getChildren(Class<T> cls) {
//        // select needed rows and ALL columns from corresponding table
//        // convert each row from ResultSet to instance of class T with appropriate id
//        // fill each of new instances with column data
//        // return list of children instances
//    }
//
//    public final <T extends Entity> List<T> getSiblings(Class<T> cls) {
//        // select needed rows and ALL columns from corresponding table
//        // convert each row from ResultSet to instance of class T with appropriate id
//        // fill each of new instances with column data
//        // return list of sibling instances
//    }



//    public final void setParent(String name, Integer id) {
//        // put parent id into fields with <name>_<id> as a key
//    }


    private void insert() throws SQLException {
        if(this.fields.isEmpty()) {
            PreparedStatement query = Entity.db.prepareStatement("INSERT INTO \"" + this.table + "\" DEFAULT VALUES " +
                    "RETURNING " + this.table + "_id;");
            ResultSet result = query.executeQuery();
            result.next();
            this.id = result.getInt(1);
            return;
        }
        String valueOrder = "";
        String values = "";
        String key = "";
        int lastIndex = this.fields.keySet().size();
        for(int i = 0; i < lastIndex; i++) {
            key = this.fields.keySet().toArray()[i].toString();
            valueOrder += key + ",\n";
            values += "\'" + this.fields.get(key) + "\',\n";
        }
        PreparedStatement query = Entity.db.prepareStatement(String.format(Entity.INSERT_QUERY,
                this.table,
                valueOrder.substring(0, valueOrder.length() - 2),
                values.substring(0, values.length() - 2))+";");
        ResultSet result = query.executeQuery();
        result.next();
        this.id = result.getInt(1);
        System.out.println(this.id);
//
//        System.out.println(this.id);

//        HashSet<String> searchedStrings = new HashSet<String>();
//        int size;
//        String columnName;
//        String columnNames = "";
//        String nullValues = "";
//        for (int i = 0; i < fields.size() ; i++) {
//            columnName = fields.keySet().toArray()[i].toString();
//            if(!columnName.endsWith("_id") &&
//               !columnName.endsWith("_created") &&
//               !columnName.endsWith("_updated")) {
//                searchedStrings.add(columnName);
//            }
//        }
//
//        int k = 0;
//        size = searchedStrings.size() - 1;
//        for (Iterator<String> it = searchedStrings.iterator(); k < size; k++) {
//            columnNames += it.next() + ", ";
//            nullValues += "null, ";
//        }
//        columnNames += searchedStrings.toArray()[searchedStrings.size()-1];
//        nullValues += "null";
//
//        if(!this.isLoaded) {
//            PreparedStatement ps = Entity.db.prepareStatement(String.format(Entity.INSERT_QUERY, this.table, columnNames, nullValues));
//            ps.execute();
//        }

    }

    private void update() throws SQLException {
        if(this.isLoaded) {
            if(this.isModified) {
                String key = "";
                String outputValues = "";
                Set<String> keys = this.fields.keySet();
                int size = keys.size() - 1;
                for (int i = 0; i < size; i++) {
                    key = keys.toArray()[i].toString();
                    Object object = this.fields.get(key);
                    outputValues += key + "=" + "\'" + object.toString() + "\'" + ",\n"; // toString() ?
                }
                outputValues += keys.toArray()[size].toString() + "=" + "\'" + this.fields.get(keys.toArray()[size]) + "\'";
                PreparedStatement query = Entity.db.prepareStatement(String.format(Entity.UPDATE_QUERY, this.table, outputValues));
                query.setInt(1, this.id);
                query.execute();
                this.isModified = false;
                return;
            }
            System.out.println("You've got the latest data.");
        }
        System.out.println("Your entity is not loaded.");

    }

    public final void delete() throws SQLException { // No results were returned Exception if executeQuery()
        String statement = String.format(Entity.DELETE_QUERY, this.table);
        PreparedStatement query = Entity.db.prepareStatement(statement);
        query.setInt(1, this.id);
        query.execute();
        this.isLoaded = false;
        this.isModified = false;
    }

    public final void save() throws SQLException {
        if(this.id == 0) {
            this.insert();
        } else {
            this.update();
        }
    }

    protected static <T extends Entity> List<T> all(Class<T> cls) {

//         select ALL rows and ALL columns from corresponding table
//         convert each row from ResultSet to instance of class T with appropriate id
//         fill each of new instances with column data
//         aggregate all new instances into a single List<T> and return it

        String tableName = cls.getSimpleName().toLowerCase();
        List<T> myList = new ArrayList<>();
        T object = null;
        Constructor<T> constructor = null;
        PreparedStatement query;
        ResultSet result;

        try {
            query = Entity.db.prepareStatement(String.format("SELECT * FROM \"%1$s\"", tableName));
            result = query.executeQuery();
            try {
                constructor = cls.getConstructor(Integer.class);
            } catch (NoSuchMethodException e) {
                System.out.println("Constructor problems.");
                e.printStackTrace();
            }
            while (result.next()) {
                object = constructor.newInstance(result.getObject(tableName + "_id"));
                myList.add(object);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

        return myList;
    }
//
//    private static Collection<String> genPlaceholders(int size) {
//        // return a string, consisting of <size> "?" symbols, joined with ", "
//        // each "?" is used in insert statements as a placeholder for values (google prepared statements)
//    }
//
//    private static Collection<String> genPlaceholders(int size, String placeholder) {
//        // return a string, consisting of <size> <placeholder> symbols, joined with ", "
//        // each <placeholder> is used in insert statements as a placeholder for values (google prepared statements)
//    }
//
//    private static String getJoinTableName(String leftTable, String rightTable) {
//        // generate the name of associative table for many-to-many relation
//        // sort left and right tables alphabetically
//        // return table name using format <table>__<table>
//    }
//
//    private java.util.Date getDate(String column) {
//        // pwoerful method, used to remove copypaste from getCreated and getUpdated methods
//    }
//
//    private static String join(Collection<String> sequence) {
//        // join collection of strings with ", " as glue and return a joined string
//    }
//
//    private static String join(Collection<String> sequence, String glue) {
//        // join collection of strings with glue and return a joined string
//    }
//
//    private static <T extends Entity> List<T> rowsToEntities(Class<T> cls, ResultSet rows) {
//        // convert a ResultSet of database rows to list of instances of corresponding class
//        // each instance must be filled with its data so that it must not produce additional queries to database to get it's fields
//    }
}
