package com.openmldb.simulator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com._4paradigm.openmldb.sdk.Column;
import com._4paradigm.openmldb.sdk.Schema;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import asg.cliche.Command;
import asg.cliche.Param;
import asg.cliche.Shell;
import asg.cliche.ShellFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * Hello world!
 *
 */
@Slf4j
public class App {
    public static final String DEFAULT_DB = "simdb";
    private Map<String, Map<String, Schema>> schemaMap = new HashMap<String, Map<String, Schema>>() {
        {
            put(DEFAULT_DB, new HashMap<String, Schema>());
        }
    };
    private String useDB = DEFAULT_DB;
    private Shell shell;

    @Command
    public String hello() {
        return "Hello, World!";
    }

    @Command(description = "use db(create if not exists)")
    public void use(@Param(name = "db") String db) {
        schemaMap.getOrDefault(db, new HashMap<String, Schema>());
        useDB = db;
        // don't use Subshells, cuz we should hold only one schemaMap, shouldn't create
        // a new App for handler
        shell.setPath(Lists.newArrayList(db)); // not a good way? simdb/db1 is better?
    }

    @Command(description = "add table to current db", abbrev = "addt")
    public void addtable(@Param(name = "table") String table, @Param(name = "schema") String schema) {
        // t1 "a int, b timestamp"
        adddbtable(useDB, table, schema);
    }

    @Command
    public void adddbtable(String db, String table, String schema) {
        log.info("add db table: {}, {}, {}", db, table, schema);
        // db1 t1 "a int, b timestamp"
        Map<String, Schema> dbMap = schemaMap.getOrDefault(db, new HashMap<String, Schema>());
        Preconditions.checkState(dbMap != null, "add db failed: " + db);
        List<Column> columnList = new ArrayList<Column>();
        String[] cols = schema.split(","); // if schema is empty, cols.length is 1, not 0
        for (int i = 0; i < cols.length; i++) {
            String[] kv = cols[i].trim().split(" ");
            Preconditions.checkState(kv.length == 2, "invalid column in schema: " + cols[i]);
            String colName = kv[0].trim();
            String colType = kv[1].trim();
            columnList.add(new Column(colName, strToType(colType)));
        }
        dbMap.put(table, new Schema(columnList));
        schemaMap.put(db, dbMap);
        // current map
        log.info("schemaMap: {}", schemaMap);
    }

    // can't parse `create table t1 (a int, b timestamp)`, cuz the part `(a int, b
    // timestamp)` should be a string
    // @Command(description = "create table in current db")
    // public void create(@Param(name = "table") String object, @Param(name =
    // "name") String name,
    // @Param(name = "schema") String schema) {}

    static Map<String, String> typeMap = ImmutableMap.of("INT", "INTEGER", "INT32", "INTEGER", "INT16", "SMALLINT",
            "INT64", "BIGINT",
            "STRING", "VARCHAR");

    private int strToType(String colType) {
        String type = colType.toUpperCase();
        if (typeMap.containsKey(type)) {
            type = typeMap.get(type);
        }
        return JDBCType.valueOf(type).getVendorTypeNumber();
    }

    @Command(description = "show all tables", abbrev = "st")
    public String showtables() {
        return Joiner.on(",").withKeyValueSeparator("=").join(schemaMap);
    }

    @Command(description = "validate sql in batch", abbrev = "val")
    public void valbatch(String sql) throws SQLException {
        List<String> ret = SqlClusterExecutor.validateSQLInBatch(sql, useDB, schemaMap);
        Preconditions.checkState(ret.isEmpty(), "validate request failed: " + ret);
    }

    @Command(description = "validate sql in request mode") // TODO how about index?
    public void valreq(String sql) throws SQLException {
        List<String> ret = SqlClusterExecutor.validateSQLInRequest(sql, useDB, schemaMap);
        Preconditions.checkState(ret.isEmpty(), "validate batch failed: " + ret);
    }

    @Command(description = "run sql in toy db engine", abbrev = "run")
    public void run(@Param(name="yaml-file") String yamlFilePath) throws SQLException, IOException {
        // toydb_run_engine better to have a version
        // execute my command
        ProcessBuilder processBuilder = new ProcessBuilder();
        URL url = com.google.common.io.Resources.getResource("toydb_run_engine");
        processBuilder.command(url.getPath(), "--yaml_path=" + yamlFilePath);
        log.info("run toydb_run_engine: {}", processBuilder.command());
        Process process = processBuilder.start();

        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }

        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(process.getErrorStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }
        Preconditions.checkState(process.exitValue() == 0, "run toydb_run_engine failed");
    }

    public void setShell(Shell shell) {
        this.shell = shell;
    }

    public static void main(String[] args) throws IOException {
        App app = new App();
        Shell shell = ShellFactory.createConsoleShell(DEFAULT_DB, "OpenMLDB Simulator", app);
        app.setShell(shell);
        shell.commandLoop();
    }

}
