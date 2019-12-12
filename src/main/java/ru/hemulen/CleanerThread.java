package ru.hemulen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CleanerThread extends Thread {
    private static Logger LOG = LoggerFactory.getLogger(CleanerThread.class.getName());
    private long interval;
    private H2Connection connection;
    public static boolean isRunnable = true;


    public CleanerThread(Properties props) {
        interval = Long.parseLong(props.getProperty("INTERVAL"));
        connection = new H2Connection(props);
    }

    public void run() {
        while (isRunnable) {
            connection.clearDB();
            try {
                sleep(interval);
            } catch (InterruptedException e) {
                LOG.error("Ошибка при попытке процесса заснуть.");
                LOG.error(e.getMessage());
            }
        }
    }
}
