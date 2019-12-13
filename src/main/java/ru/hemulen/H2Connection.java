package ru.hemulen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.plaf.nimbus.State;
import java.sql.*;
import java.util.Properties;

public class H2Connection {
    private static Logger LOG = LoggerFactory.getLogger(H2Connection.class.getName());
    private Connection conn;

    public H2Connection(Properties props) {
        // Устанавливаем подключение к базе данных
        String h2Path = props.getProperty("H2PATH");
        String H2Url = "jdbc:h2:file:" + h2Path + ";IFEXISTS=TRUE;AUTO_SERVER=TRUE";
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            LOG.error("Не удалось найти класс драйвера H2.");
            LOG.error(e.getMessage());
        }
        try {
            LOG.info("Начало подключения к базе данных H2.");
            conn = DriverManager.getConnection(H2Url, props.getProperty("USER"), props.getProperty("PASS"));
            LOG.info("Подключение к базе данных H2 установлено.");
        } catch (SQLException e) {
            LOG.error(String.format("Не удалось подключиться к базе данных ", h2Path));
            LOG.error(e.getMessage());
            System.exit(1);
        }
    }

    public void close() {
        try {
            conn.close();
            LOG.info("Подключение к базе данных H2 закрыто.");
        } catch (SQLException e) {
            LOG.error("Не удалось закрыть соединение с базой данных H2.");
            LOG.error(e.getMessage());
        }
    }

    /**
     * Метод удаляет из базы данных все ответы, на которые получены финальные ответы ('REJECT', 'ERROR' или 'MESSAGE'),
     * а также все ответы, связанные с этими запросами (включая STATUS).
     */
    public void clearDB() {
        LOG.info("Начало очистки базы данных H2:");
        try {
            getWasteRequests();
            getWasteResponses();
            deleteMessages();
        } catch (SQLException e) {
            LOG.error(e.getMessage());
            return;
        } finally {
            dropTempTables();
        }
        LOG.info("База данных H2 очищена.");
    }

    /**
     * Метод создает таблицу REQ_TMP и наполняет ее идентификаторами запросов,
     * на которые получены финальные ответы ('REJECT', 'ERROR' или 'MESSAGE')
     *
     * @return Количество всех запросов в финальном статусе
     */
    private void getWasteRequests() throws SQLException {
        try {
            String sql = "CREATE TABLE REQ_TMP AS \n" +
                    "(SELECT MM.REFERENCE_ID\n" +
                    "FROM CORE.MESSAGE_METADATA MM\n" +
                    "LEFT JOIN CORE.MESSAGE_CONTENT MC ON MC.ID = MM.ID\n" +
                    "WHERE MC.MODE IN ('MESSAGE', 'REJECT', 'ERROR')\n" +
                    "AND MM.MESSAGE_TYPE = 'RESPONSE')";
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql);
            LOG.info("Идентификаторы запросов в финальных статусах выбраны в таблицу REQ_TMP.");
        } catch (SQLException e) {
            LOG.error("Не удалось извлечь идентификаторы запросов в финальных статусах.");
            throw e;
        }
    }

    /**
     * Метод создает таблицу RESP_TMP и наполняет ее идентификаторами всех ответов на запросы,
     * находящихся в финальном статусе, включая ответы типа STATUS.
     *
     * Метод должен вызываться после метода getWasteRequests!
     *
     * @return Количество всех ответов на запросы, находящиеся в финальном статусе
     */
    private void getWasteResponses() throws SQLException {
        try {
            String sql = "CREATE TABLE RESP_TMP AS \n" +
                    "SELECT ID \n" +
                    "FROM CORE.MESSAGE_METADATA\n" +
                    "WHERE REFERENCE_ID IN \n" +
                    "(SELECT REFERENCE_ID FROM REQ_TMP)";
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql);
            LOG.info("Идентификаторы ответов на запросы в финальных статусах выбраны в таблицу RESP_TMP.");
        } catch (SQLException e) {
            LOG.error("Не удалось извлечь идентификаторы ответов на запросы в финальных статусах.");
            throw e;
        }
    }

    /**
     * Метод сначала удаляет все записи в базе данных, связанные с ответами на запросы в финальных статусах.
     * Затем метод удаляет все записи в базе данных, связанные с запросами в финальных статусах.
     * В конце метод удаляет временные таблицы RESP_TMP и REQ_TMP.
     */
    private void deleteMessages() throws SQLException {
        try {
            // Подготавливаем запросы на удаление ответов
            String sqlRespAttachments = "DELETE FROM CORE.ATTACHMENT_METADATA WHERE MESSAGE_METADATA_ID IN (SELECT ID FROM RESP_TMP);";
            String sqlRespMessageContent = "DELETE FROM CORE.MESSAGE_CONTENT WHERE ID IN (SELECT ID FROM RESP_TMP);";
            String sqlRespMessageMetadata = "DELETE FROM CORE.MESSAGE_METADATA WHERE ID IN (SELECT ID FROM RESP_TMP);";
            String sqlRespMessageState = "DELETE FROM CORE.MESSAGE_STATE WHERE ID IN (SELECT ID FROM RESP_TMP);";
            // Создаем запрос
            Statement statement = conn.createStatement();
            // И выполняем запросы на удаление из разных таблиц
            statement.executeUpdate(sqlRespAttachments);
            LOG.info("Удалены ответы из ATTACHMENT_METADATA.");
            statement.executeUpdate(sqlRespMessageContent);
            LOG.info("Удалены ответы из MESSAGE_CONTENT.");
            statement.executeUpdate(sqlRespMessageMetadata);
            LOG.info("Удалены ответы из MESSAGE_METADATA.");
            statement.executeUpdate(sqlRespMessageState);
            LOG.info("Удалены ответы из MESSAGE_STATE.");

            // Подготавливаем запросы на удаление запросов
            String sqlReqAttachments = "DELETE FROM CORE.ATTACHMENT_METADATA WHERE MESSAGE_METADATA_ID IN (SELECT REFERENCE_ID FROM REQ_TMP);";
            String sqlReqMessageContent = "DELETE FROM CORE.MESSAGE_CONTENT WHERE ID IN (SELECT REFERENCE_ID FROM REQ_TMP);";
            String sqlReqMessageMetadata = "DELETE FROM CORE.MESSAGE_METADATA WHERE ID IN (SELECT REFERENCE_ID FROM REQ_TMP);";
            String sqlReqMessageState = "DELETE FROM CORE.MESSAGE_STATE WHERE ID IN (SELECT REFERENCE_ID FROM REQ_TMP);";

            // И выполняем запросы на удаление из разных таблиц
            statement.executeUpdate(sqlReqAttachments);
            LOG.info("Удалены запросы из ATTACHMENT_METADATA.");
            statement.executeUpdate(sqlReqMessageContent);
            LOG.info("Удалены запросы из MESSAGE_CONTENT.");
            statement.executeUpdate(sqlReqMessageMetadata);
            LOG.info("Удалены запросы из MESSAGE_METADATA.");
            statement.executeUpdate(sqlReqMessageState);
            LOG.info("Удалены запросы из MESSAGE_STATE.");
        } catch (SQLException e) {
            LOG.error("Не удалось удалить записи.");
            throw e;
        }
    }

    private void dropTempTables() {
        try {
            String sqlDropTables = "DROP TABLE REQ_TMP; DROP TABLE RESP_TMP;";
            Statement statement = conn.createStatement();
            statement.executeUpdate(sqlDropTables);
            LOG.info("Временные таблицы удалены.");
        } catch (SQLException e) {
            LOG.error("Не удалось удалить временные таблицы.");
        }
    }
}
