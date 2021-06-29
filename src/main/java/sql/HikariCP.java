package sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class HikariCP {
  private static HikariConfig config = new HikariConfig();
  private static HikariDataSource ds;
  private static final Logger logger = LogManager.getLogger(HikariCP.class.getName());

  private HikariCP() {
  }

  static {
    Properties prop = new Properties();
    try (InputStream input = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("config.properties")) {
      prop.load(input);
    } catch (IOException ex) {
      ex.printStackTrace();
    }

    final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    final String HOST_NAME = prop.getProperty("mysql.ipaddress");
    final String PORT = prop.getProperty("mysql.port");
    final String DATABASE = "wcdb2";
    final String USERNAME = prop.getProperty("mysql.username");
    final String PASSWORD = prop.getProperty("mysql.password");

    String url = String
        .format("jdbc:mysql://%s:%s/%s?serverTimezone=UTC", HOST_NAME, PORT, DATABASE);
//
//    try {
//      Class.forName("com.mysql.cj.jdbc.Driver");
//      logger.info("find class for name");
//    } catch (ClassNotFoundException e) {
//      logger.info(e.getMessage());
//    }

    config.setJdbcUrl(url);
    config.setUsername(USERNAME);
    config.setPassword(PASSWORD);
    config.setDriverClassName(JDBC_DRIVER);
    config.addDataSourceProperty("cachePrepStmts", "true");
    config.addDataSourceProperty("prepStmtCacheSize", "250");
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    config.addDataSourceProperty("useServerPrepStmts", "true");
    config.addDataSourceProperty("useLocalSessionState", "true");
    config.addDataSourceProperty("rewriteBatchedStatements", "true");
    config.addDataSourceProperty("cacheResultSetMetadata", "true");
    config.addDataSourceProperty("cacheServerConfiguration", "true");
    config.addDataSourceProperty("elideSetAutoCommits", "true");
    config.addDataSourceProperty("maintainTimeStats", "false");
    ds = new HikariDataSource(config);
    ds.setMaximumPoolSize(60);
  }

  public static HikariDataSource getDataSource() {
    return ds;
  }

  public static Connection getConnection() throws SQLException {
    return ds.getConnection();
  }
}
