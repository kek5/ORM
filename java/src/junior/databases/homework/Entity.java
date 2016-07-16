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
    public final void setTableName(String tableName) {
        this.table = tableName;
    }
    public final void printMap() {
        String key = "";
        for (int i = 0; i < this.fields.size(); i++) {
            key = this.fields.keySet().toArray()[i].toString();
            System.out.print(key + "+++");
            System.out.println(this.fields.get(key));
        }
    }
    private final ResultSet children_siblings_getResultSet(String statement) {
        try {
            PreparedStatement query = Entity.db.prepareStatement(statement);
            query.setInt(1, (int)this.fields.get(this.table + "_id"));
            ResultSet result = query.executeQuery();
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
    // the end

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
        return this.getDate(this.table + "_created");
    }
    public final java.util.Date getUpdated() {
        return this.getDate(this.table + "_updated");
    }

    public final Object getColumn(String name) {
        try{
            if(this.isLoaded) {
                if(this.isModified) {
                    this.save();
                    this.isModified = false;
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
    public final <T extends Entity> T getParent(Class<T> cls) {
        // get parent id from fields as <classname>_id, create and return an instance of class T with that id
        Constructor<T> constructor;
        T parent = null;

        try {
            constructor = cls.getConstructor(Integer.class);
            String key = cls.getSimpleName().toLowerCase() + "_id";
            parent = constructor.newInstance(this.fields.get(key));
        } catch (NoSuchMethodException e) {
            System.out.println("Constructor problems.");
            e.printStackTrace();
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }

        return parent;
    }

    public final <T extends Entity> List<T> getChildren(Class<T> cls) {
        // select needed rows and ALL columns from corresponding table
        // convert each row from ResultSet to instance of class T with appropriate id
        // fill each of new instances with column data
        // return list of children instances
        String classTableName = cls.getSimpleName().toLowerCase();
        String statement = String.format(Entity.CHILDREN_QUERY, classTableName, this.table);
        return Entity.rowsToEntities(cls, this.children_siblings_getResultSet(statement));
    }

    public final <T extends Entity> List<T> getSiblings(Class<T> cls) {
        // select needed rows and ALL columns from corresponding table
        // convert each row from ResultSet to instance of class T with appropriate id
        // fill each of new instances with column data
        // return list of sibling instances
        String classTableName = cls.getSimpleName().toLowerCase();
        String statement = String.format(Entity.SIBLINGS_QUERY, Entity.getJoinTableName(this.table, classTableName), classTableName, this.table);

        return Entity.rowsToEntities(cls, this.children_siblings_getResultSet(statement));
    }



    public final void setParent(String name, Integer id) {
        // put parent id into fields with <name>_<id> as a key
        this.fields.put(name + "_id", id);
    }

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
        String key;
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
        T object;
        Constructor<T> constructor = null;
        PreparedStatement query;
        ResultSet result;

        try {
            query = Entity.db.prepareStatement(String.format(Entity.LIST_QUERY, tableName));
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
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException | SQLException e) {
            e.printStackTrace();
        }

        return myList;
    }
//
    private static Collection<String> genPlaceholders(int size) {
        // return a string, consisting of <size> "?" symbols, joined with ", "
        // each "?" is used in insert statements as a placeholder for values (google prepared statements)
        return Entity.genPlaceholders(size, "?");
    }

    private static Collection<String> genPlaceholders(int size, String placeholder) {
        // return a string, consisting of <size> <placeholder> symbols, joined with ", "
        // each <placeholder> is used in insert statements as a placeholder for values (google prepared statements)
        List<String> temp = new ArrayList<>(2);
        temp.add(0, String.valueOf(size));
        temp.add(1, placeholder);
        return new ArrayList<>(Arrays.asList(Entity.join(temp)));
    }

    private static String getJoinTableName(String leftTable, String rightTable) {
        // generate the name of associative table for many-to-many relation
        // sort left and right tables alphabetically
        // return table name using format <table>__<table>
        if(leftTable.charAt(0) < rightTable.charAt(0)) {
            return leftTable + "__" + rightTable;
        }
        return rightTable + "__" + leftTable;
    }

    private java.util.Date getDate(String column) {
        String dateString;
        java.util.Date date = null;
        SimpleDateFormat formatter = new SimpleDateFormat("EEEE, MMMM d,yyyy h:mm,a", Locale.ENGLISH);
        formatter.setTimeZone(TimeZone.getTimeZone("GMT+2"));
        String statement = "select " + column + " from " + "\"" + this.table + "\"" + " WHERE " + this.table + "_id =" + this.id;
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

    private static String join(Collection<String> sequence) {
        return Entity.join(sequence, ", ");
    }

    private static String join(Collection<String> sequence, String glue) {
        // join collection of strings with glue and return a joined string
        int last = Integer.parseInt((String)sequence.toArray()[0]) - 1;
        String temp = "";
        for (int i = 0; i < last; i++) {
            temp += sequence.toArray()[1] + glue;
        }
        temp += sequence.toArray()[1];

        return temp;
    }

    private static <T extends Entity> List<T> rowsToEntities(Class<T> cls, ResultSet result) {
        // convert a ResultSet of database rows to list of instances of corresponding class
        // each instance must be filled with its data so that it must not produce additional queries to database to get it's fields
        String classTableName = cls.getSimpleName().toLowerCase();
        List<T> list = null;

        try {
            list = new ArrayList<>();
            T entity;
            Constructor<T> constructor = cls.getConstructor(Integer.class);
            while(result.next()) {
                entity = constructor.newInstance(result.getInt(classTableName + "_id"));
                list.add(entity);
            }
        } catch (SQLException | InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
            e.printStackTrace();
        }

        return list;
    }

}
