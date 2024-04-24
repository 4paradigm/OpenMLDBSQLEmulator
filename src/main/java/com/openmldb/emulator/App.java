package com.openmldb.emulator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.yaml.snakeyaml.Yaml;

import com._4paradigm.openmldb.sdk.Column;
import com._4paradigm.openmldb.sdk.Schema;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
    public static final String DEFAULT_DB = "emudb";
    private Map<String, Map<String, Schema>> schemaMap = new HashMap<String, Map<String, Schema>>() {
        {
            put(DEFAULT_DB, new HashMap<String, Schema>());
        }
    };
    private String useDB = DEFAULT_DB;
    private Shell shell;

    private String caseYamlPath = "/tmp/emu-case.yaml";

    // logic about toydb binary
    public static final String tempToy = "/tmp/toydb_run_engine";

    static {
        // extractResource to run it when jar
        File copied = new File(tempToy);
        if (copied.exists()) {
            System.out.println(tempToy + " exists, skip extract(if file is wrong, delete it)");
        } else {
            System.out.println("extract toydb_run_engine to " + tempToy);
            try (FileOutputStream fout = new FileOutputStream(copied);) {
                URL url = com.google.common.io.Resources.getResource("toydb_run_engine");
                com.google.common.io.Resources.copy(url, fout);
                Set<PosixFilePermission> perms = new HashSet<>();
                perms.add(PosixFilePermission.OWNER_EXECUTE);
                perms.add(PosixFilePermission.OWNER_READ);
                Files.setPosixFilePermissions(copied.toPath(), perms);
            } catch (Exception e) {
                log.error("extract toydb_run_engine failed, so no toydb support", e);
                // create fout will create the file
                copied.delete();
            }
        }
    }

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
        shell.setPath(Lists.newArrayList(db)); // TODO(hw): 'emudb/db1' is better?
    }

    @Command(description = "add table to current db", abbrev = "t")
    public void addtable(@Param(name = "table") String table, @Param(name = "schema") String... schema) {
        // t1 "a int, b timestamp"
        adddbtable(useDB, table, schema);
    }

    @Command(description = "add table to specified db", abbrev = "dt")
    public void adddbtable(@Param(name = "db") String db, @Param(name = "table") String table,
            @Param(name = "schema") String... schemaParts) {
        String schema = Joiner.on(" ").join(schemaParts);
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

    @Command(description = "exec sql, not full support")
    public void sql(@Param(name = "sql") String... sqlParts) {
        String sql = Joiner.on(" ").join(sqlParts);
        if (sql.toLowerCase().startsWith("create table ")) {
            sql = sql.substring("create table ".length());
            log.info("create table parse: {}", sql);
            String[] parts = sql.split("\\(", 2);
            String table = parts[0].trim();
            String[] tableParts = table.split(".");
            // schema has `)`
            String schema = parts[1].trim();
            Preconditions.checkState(schema.endsWith(")"),
                    "invalid schema: " + schema + ", should start with ( and end with )");
            schema = schema.substring(0, schema.length() - 1);
            if (tableParts.length == 2) {
                adddbtable(parts[0], parts[1], schema);
            } else {
                adddbtable(useDB, table, schema);
            }
        } else {
            throw new UnsupportedOperationException("not support sql: " + sql);
        }
    }

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
    public void valbatch(@Param(name = "sql") String... sqlParts) throws SQLException {
        String sql = Joiner.on(" ").join(sqlParts);
        List<String> ret = SqlClusterExecutor.validateSQLInBatch(sql, useDB, schemaMap);
        Preconditions.checkState(ret.isEmpty(), "validate batch failed: " + ret);
        System.out.println("validate batch success");
    }

    @Command(description = "validate sql in request mode") // no need to prepare deployment indexs
    public void valreq(@Param(name = "sql") String... sqlParts) throws SQLException {
        String sql = Joiner.on(" ").join(sqlParts);
        List<String> ret = SqlClusterExecutor.validateSQLInRequest(sql, useDB, schemaMap);
        Preconditions.checkState(ret.isEmpty(), "validate request failed: " + ret);
        System.out.println("validate request success");
    }

    @Command(description = "run sql in toy db engine, if no file, run cache path")
    public void run() throws SQLException, IOException {
        run(caseYamlPath);
    }

    @Command(description = "run sql in toy db engine, if no file, run cache path")
    public void run(@Param(name = "yaml-file") String yamlFilePath) throws SQLException, IOException {
        // toydb_run_engine better to have a version
        // execute my command
        if (!toydbSupport()) {
            System.out.println("no toydb_run_engine, build it first");
            return;
        }
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(tempToy, "--yaml_path=" + yamlFilePath);
        System.out.println("run toydb_run_engine: " + processBuilder.command());
        Process process = processBuilder.start();
        try {
            process.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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

    private boolean toydbSupport() {
        File toy = new File(tempToy);
        return toy.exists();
    }

    @Command(description = "generate create table sqls, can't support multi db cuz openmldb-jdbc, sql can't have <db>.<table>, just table name")
    public void genddl(@Param(name = "sql", description = "deployment sql") String... sqlParts)
            throws SQLException {
        String sql = Joiner.on(" ").join(sqlParts);
        log.info("gen ddl: {}", sql);
        List<String> ddl = SqlClusterExecutor.genDDL(sql, schemaMap);
        System.out.println(Joiner.on("\n").join(ddl));
    }

    @Command(description = "gen the yaml file with one template case and store the path")
    public void gencase() throws IOException {
        gencase(caseYamlPath);
    }

    @Command(description = "gen the yaml file with one template case and store the path, if emtpy, write to the cache path")
    public void gencase(@Param(name = "output") String outputPath) throws IOException {
        // open resource as stream
        URL url = com.google.common.io.Resources.getResource("case-temp.yaml");
        if (outputPath.isEmpty()) {
            outputPath = caseYamlPath;
        }
        // copy file
        File copied = new File(outputPath);
        FileOutputStream fout = new FileOutputStream(copied);
        com.google.common.io.Resources.copy(url, fout);
        Preconditions.checkState(copied.exists(), "copy case-temp.yaml failed, to " + caseYamlPath);
        caseYamlPath = outputPath;
        System.out.println("copy case-temp.yaml to " + caseYamlPath + ", and set it to yaml cache path");
    }

    private Map<String, Object> readYaml(String yamlPath) throws IOException {
        InputStream inputStream = new FileInputStream(new File(yamlPath));
        Yaml yaml = new Yaml();
        Map<String, Object> data = yaml.load(inputStream);
        return data;
    }

    private void writeYaml(String yamlPath, Map<String, Object> data) throws IOException {
        Yaml yaml = new Yaml();
        Writer writer = new OutputStreamWriter(new FileOutputStream(yamlPath), "UTF-8");
        yaml.dump(data, writer);
    }

    @Command(description = "load table schemas from yaml and change the yaml cache path")
    public void loadcase() throws IOException {
        loadcase(caseYamlPath);
    }

    @Command(description = "load table schemas from yaml and change the yaml cache path, if emtpy, load from cache path")
    public void loadcase(@Param(name = "input") String inputPath) throws IOException {
        if (inputPath.isEmpty()) {
            inputPath = caseYamlPath;
        }
        // load yaml
        Map<String, Object> data = readYaml(inputPath);
        System.out.println(data);
        // global db
        String globalDB = "";
        if (data.containsKey("db")) {
            globalDB = (String) data.get("db");
        }

        // for each case, case db
        // read each case? or support only one case
        List<Map<String, Object>> cases = (List<Map<String, Object>>) Preconditions.checkNotNull(data.get("cases"));
        Preconditions.checkState(cases.size() == 1);
        // tables ignore indexs and data
        String caseDB = "";
        if (cases.get(0).containsKey("db")) {
            caseDB = (String) cases.get(0).get("db");
        }
        String defaultDB = caseDB;
        if (defaultDB.isEmpty()) {
            defaultDB = globalDB;
        }
        List<Map<String, Object>> tables = (List<Map<String, Object>>) Preconditions
                .checkNotNull(cases.get(0).get("inputs"));
        for (Map<String, Object> table : tables) {
            String db = (String) table.get("db");
            if (db == null || db.isEmpty()) {
                db = defaultDB;
            }
            String name = (String) table.get("name");
            List<String> schema = (List<String>) table.get("columns");
            adddbtable(db, name, Joiner.on(",").join(schema));
        }

        caseYamlPath = inputPath;
        System.out.println("load case from " + inputPath + ", and set it to yaml cache path");
    }

    @Command(description = "dump sql to yaml and save it to cache path, should change index?")
    public void dumpcase(@Param(name = "sql") String... sqlParts) throws IOException {
        String sql = Joiner.on(" ").join(sqlParts);
        Map<String, Object> data = readYaml(caseYamlPath);
        // dump yaml
        List<Map<String, Object>> cases = (List<Map<String, Object>>) Preconditions
                .checkNotNull(data.get("cases"));
        Preconditions.checkState(cases.size() == 1);
        Map<String, Object> case0 = cases.get(0);
        case0.put("sql", sql);
        // must gen index for tables for toydb init, no need to be the deployment indexs
        List<Map<String, Object>> tables = (List<Map<String, Object>>) Preconditions
                .checkNotNull(case0.get("inputs"));
        for (Map<String, Object> table : tables) {
            // multi indexs is a list
            List<String> indexs = (List<String>) table.get("indexs");
            List<String> cols = (List<String>) table.get("columns");
            // if user set indexs, use it, if empty, add index0
            if (indexs.isEmpty())
                indexs.add("index0:" + cols.get(0).split(" ")[0]);
        }

        log.info("dump case: {}", cases);
        writeYaml(caseYamlPath, data);
        System.out.println("dump sql to " + caseYamlPath);
    }

    public void setShell(Shell shell) {
        this.shell = shell;
    }

    public static void main(String[] args) throws IOException {
        App app = new App();
        Shell shell = ShellFactory.createConsoleShell(DEFAULT_DB, "OpenMLDB SQL Emulator", app);
        app.setShell(shell);
        shell.commandLoop();
    }

}
