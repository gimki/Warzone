package com.company;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class LoggerTool {

    static Logger setupLogger(String name) {
        Logger logger = Logger.getLogger(name);
        FileHandler fh;
        try {
            fh = new FileHandler("/tmp/cpu-bound-dummy-application.log", true);
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);

        } catch (SecurityException se) {
            throw se;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return logger;
    }
}
