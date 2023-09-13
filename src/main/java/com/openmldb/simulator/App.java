package com.openmldb.simulator;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com._4paradigm.openmldb.sdk.Schema;

import asg.cliche.Command;
import asg.cliche.ShellFactory;

/**
 * Hello world!
 *
 */
public class App {
    private Map<String, Map<String, Schema>> schemaMap = new HashMap<String, Map<String, Schema>>();
    private String defaultDB = "default";

    @Command // One,
    public String hello() {
        return "Hello, World!";
    }

    @Command // two,
    public int add(int a, int b) {
        return a + b;
    }

    @Command
    public void addtable(String table, String schema) {
        // t1 "a int, b timestamp"
        adddbtable(defaultDB, table, schema);
    }

    @Command
    public void adddbtable(String db, String table, String schema) {
        // db1 t1 "a int, b timestamp"
        Map<String, Schema> dbMap = schemaMap.putIfAbsent(db, new HashMap<String, Schema>());
        List<Column> columnList = new ArrayList<Column>();
        String[] cols = schema.split(",");
        for (int i = 0; i < cols.length; i++) {
            String[] kv = cols[i].trim().split(" ");
            String colName = kv[0].trim();
            String colType = kv[1].trim();
            columnList.add(new Column(colName, colType));
        }
        Schema res = new Schema(columnList);
        dbMap.put(table, res);
    }

    @Command
    public void valreq(String sql) {
        SqlClusterExecutor.validateSQLInRequest(sql, schemaMap);
    }

    public static void main(String[] args) throws IOException {
        ShellFactory.createConsoleShell("sim", "", new App())
                .commandLoop(); // and three.
    }
}
