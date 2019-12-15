package ru.hemulen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Main {
    private static Logger LOG = LoggerFactory.getLogger(Main.class.getName());

    public static void main(String[] args) {
        // Читаем настройки
        Properties props = new Properties();
        try {
            props.load(new FileInputStream("./config/config.ini"));
        } catch (IOException e) {
            LOG.error("Не удалось прочитать файл конфигурации.");
            LOG.error(e.getMessage());
            System.exit(1);
        }
        // Создаем подключение к БД с этими настройками
        H2Connection connection = new H2Connection(props);
        // Выполняем очистку базы данных
        connection.clearDB();
        // Закрываем подключение
        connection.close();
    }
}
