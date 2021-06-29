package sql;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.ResultSet;
import javax.xml.crypto.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class WordCountDao {

  private static final Logger logger = LogManager.getLogger(WordCountDao.class.getName());
  private static HikariDataSource dataSource = HikariCP.getDataSource();

  public WordCountDao() {
  }

  public Integer getWordCount(String tableName, String word) {
    String getQueryStatement = "SELECT SUM(wordCount) FROM "
        + tableName
        + " WHERE wordKey = "
        + "'" + word + "';";

    try (Connection conn = HikariCP.getConnection();
        Statement stmt = conn.createStatement();
    ) {
      ResultSet result = stmt.executeQuery(getQueryStatement);
      Integer wordCt = 0;
      if (result.next()) {
        wordCt = result.getInt(1);
      }
      return wordCt;
    } catch (SQLException e) {
      logger.info(e.getMessage());
      return -1;
    }
  }
}

