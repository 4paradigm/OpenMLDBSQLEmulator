package com.openmldb.emulator;

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
import com.openmldb.emulator.App;

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
        assertTrue(show, show.equals("emudb={},d1={t1=c1:int32, t2=c1:int32},d2={t1=c1:int32}"));
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

    private void runScript(String script) throws IOException, CLIException {
        App app = new App();
        Shell shell = ShellFactory.createConsoleShell(App.DEFAULT_DB, "test", app);
        app.setShell(shell);
        BufferedReader reader = asBufferedReader(script);
        String line = null;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            System.out.println("test executing: " + line);
            shell.processLine(line);
        }
    }

    @Test
    public void testSimScripts() throws IOException, CLIException {
        runScript("simple.emu");
        runScript("case.emu");
    }

    @Test
    public void testYamlCases() throws SQLException, IOException {
        App app = new App();
        app.run(com.google.common.io.Resources.getResource("case.yaml").getFile());
    }
}
