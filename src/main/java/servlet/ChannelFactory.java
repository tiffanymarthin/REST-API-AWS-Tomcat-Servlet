package servlet;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class is responsible for managing the lifecycle of a pooled channel
 */
public class ChannelFactory extends BasePooledObjectFactory<Channel> {

  private static final Logger logger = LogManager.getLogger(ChannelFactory.class.getName());

  private final Connection connection;

  /**
   * Constructor that si responsible of creating the connection to RabbitMQ Using property file to
   * store rabbitMQ configuration
   *
   * @throws IOException      when there is IO problem with config file
   * @throws TimeoutException when timeout exception caught during connection creation
   */
  ChannelFactory() throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();

    Properties prop = new Properties();
    try (InputStream input = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("config.properties")) {
      prop.load(input);
    } catch (IOException ex) {
      logger.info("IO Exception while reading config file");
    }

    factory.setUsername(prop.getProperty("rabbit.username"));
    factory.setPassword(prop.getProperty("rabbit.password"));
    factory.setHost(prop.getProperty("rabbit.ip"));
    connection = factory.newConnection();
  }

  /**
   * Creates rabbitMQ channel
   *
   * @return the created Channel
   */
  @Override
  public Channel create() throws IOException {
    return connection.createChannel();
  }

  /**
   * Wrap specified channel inside a pooled object
   *
   * @param channel specified channel to be wrapped
   * @return channel wrapped in pooled object
   */
  @Override
  public PooledObject<Channel> wrap(Channel channel) {
    return new DefaultPooledObject<>(channel);
  }

}
