package ru.hemulen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            conn = DriverManager.getConnection(H2Url, props.getProperty("USER"), props.getProperty("PASS"));
        } catch (SQLException e) {
            LOG.error(String.format("Не удалось подключиться к базе данных ", h2Path));
            LOG.error(e.getMessage());
            System.exit(1);
        }
    }

    public void close() {
        try {
            conn.close();
            LOG.info("Соединение с базой данных H2 закрыто.");
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
        ResultSet wasteResponses = getWasteResponses();
        ResultSet wasteRequests = getWasteRequests();
        // Сначала удаляем ответы
        LOG.info("Начало удаления ответов на запросы в финальных статусах.");
        deleteMessages(wasteResponses);
        LOG.info("Ответы на запросы в финальных статусах удалены.");
        // Потом удаляем запросы
        LOG.info("Начало удаления запросов в финальных статусах.");
        deleteMessages(wasteRequests);
        LOG.info("Запросы в финальных статусах удалены.");
        LOG.info("База данных H2 очищена.");
    }

    /**
     * Метод возвращает набор идентификаторов запросов,
     * на которые получены финальные ответы ('REJECT', 'ERROR' или 'MESSAGE')
     *
     * @return Набор идентификаторов запросов в финальных статусах
     */
    private ResultSet getWasteRequests() {
        try {
            String sql = "SELECT \n" +
                    "MM.REFERENCE_ID\n" +
                    "FROM CORE.MESSAGE_CONTENT MC\n" +
                    "LEFT JOIN CORE.MESSAGE_METADATA MM ON MC.ID = MM.ID\n" +
                    "WHERE MC.MODE IN ('MESSAGE', 'REJECT', 'ERROR')\n" +
                    "AND MM.MESSAGE_TYPE = 'RESPONSE'";
            Statement stmt = conn.createStatement();
            ResultSet result = stmt.executeQuery(sql);
            LOG.info("Идентификаторы запросов в финальных статусах выбраны.");
            return result;
        } catch (SQLException e) {
            LOG.error("Не удалось извлечь идентификаторы запросов в финальных статусах.");
            LOG.error(e.getMessage());
            return null;
        }
    }

    /**
     * Метод выбирает идентификаторы всех ответов на запросы, находящихся в финальном статусе,
     * включая ответы типа STATUS.
     * @return Идентификаторы всех ответов на запросы, находящиеся в финальном статусе
     */
    private ResultSet getWasteResponses() {
        try {
            String sql = "-- Выбираем идентификаторы всех ответов на запросы в финальном статусе\n" +
                    "SELECT ID \n" +
                    "FROM CORE.MESSAGE_METADATA\n" +
                    "WHERE REFERENCE_ID IN \n" +
                    "(\n" +
                    "   -- Выбираем идентификаторы запросов, на которые получены ответы типа 'MESSAGE', 'REJECT' и 'ERROR'\n" +
                    "   SELECT \n" +
                    "   MM.REFERENCE_ID\n" +
                    "   FROM CORE.MESSAGE_CONTENT MC\n" +
                    "   LEFT JOIN CORE.MESSAGE_METADATA MM ON MC.ID = MM.ID\n" +
                    "   WHERE MC.MODE IN ('MESSAGE', 'REJECT', 'ERROR')\n" +
                    "   AND MM.MESSAGE_TYPE = 'RESPONSE'\n" +
                    ")";
            Statement stmt = conn.createStatement();
            ResultSet result = stmt.executeQuery(sql);
            LOG.info("Идентификаторы ответов на запросы в финальных статусах выбраны.");
            return result;
        } catch (SQLException e) {
            LOG.error("Не удалось извлечь идентификаторы ответов на запросы в финальных статусах.");
            LOG.error(e.getMessage());
            return null;
        }
    }

    /**
     * Метод удаляет все записи в базе данных, связанные с определенными сообщениями.
     *
     * @param ids2delete ResultSet с набором идентификаторов сообщений, которые необходимо удалить
     */
    private void deleteMessages(ResultSet ids2delete) {
        try {
            // Подготавливаем запросы
            String sqlMessageContent = "DELETE FROM CORE.MESSAGE_CONTENT WHERE ID = ?";
            String sqlAttachments = "DELETE FROM CORE.ATTACHMENT_METADATA WHERE MESSAGE_METADATA_ID = ?";
            String sqlMessageState = "DELETE FROM CORE.MESSAGE_STATE WHERE ID = ?";
            String sqlMessageMetadata = "DELETE FROM CORE.MESSAGE_METADATA WHERE ID = ?";
            PreparedStatement psAttachments = conn.prepareStatement(sqlAttachments);
            PreparedStatement psMessageContent = conn.prepareStatement(sqlMessageContent);
            PreparedStatement psMessageMetadata = conn.prepareStatement(sqlMessageMetadata);
            PreparedStatement psMessageState = conn.prepareStatement(sqlMessageState);
            while (ids2delete.next()) {
                String clientID = ids2delete.getString(1);
                psMessageContent.setString(1, clientID);
                psAttachments.setString(1, clientID);
                psMessageState.setString(1, clientID);
                psMessageMetadata.setString(1, clientID);
                psAttachments.execute();
                psMessageContent.execute();
                psMessageMetadata.execute();
                psMessageState.execute();
            }
        } catch (SQLException e) {
            LOG.error("Не удалось удалить записи.");
            LOG.error(e.getMessage());
        }
    }

}
