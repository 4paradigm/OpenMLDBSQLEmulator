package com.openmldb.simulator;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;

import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.CharSource;
import com.google.common.io.CharStreams;
import com.google.common.io.Resources;

import asg.cliche.CLIException;
import asg.cliche.Shell;
import asg.cliche.ShellFactory;

/**
 * Unit test for simple App.
 */
public class AppTest {
    /**
     * Rigorous Test :-)
     */
    @Test
    public void showtables() {
        App app = new App();
        try {
            app.adddbtable("d1", "t1", "");
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(e.getMessage(), e.getMessage().contains("invalid column in schema"));
        }
        app.adddbtable("d1", "t1", "c1 int");
        app.adddbtable("d1", "t2", "c1 int");
        app.adddbtable("d2", "t1", "c1 int");
        String show = app.showtables();
        assertTrue(show, show.equals("simdb={},d1={t1=c1:int32, t2=c1:int32},d2={t1=c1:int32}"));
    }

    public static BufferedReader asBufferedReader(String resource) {
        URL url = com.google.common.io.Resources.getResource(resource);
        try {
            CharSource charSource = com.google.common.io.Resources.asCharSource(url, Charsets.UTF_8);
            return charSource.openBufferedStream();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testSimScripts() throws IOException, CLIException {
        // read resouces file
        App app = new App();
        Shell shell = ShellFactory.createConsoleShell(App.DEFAULT_DB, "test", app);
        app.setShell(shell);
        BufferedReader reader = asBufferedReader("simple.sim");
        String scripts = CharStreams.toString(reader);
        String[] lines = scripts.split("\n");
        for (String line : lines) {
            if (line.startsWith("#")) {
                continue;
            }
            System.out.println("executing: " + line);
            shell.processLine(line);
        }
    }

    @Test
    public void testYamlCases() throws SQLException, IOException {
        App app = new App();
        app.run(com.google.common.io.Resources.getResource("simple.yaml").getFile());
    }
}
