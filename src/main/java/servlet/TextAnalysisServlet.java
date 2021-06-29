package servlet;

import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.annotation.*;
import java.io.IOException;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sql.WordCountDao;

/**
 * This class is the Java Servlet implementation for analyzing texts
 */
@WebServlet(name = "TextAnalysisServlet", urlPatterns = "/textbody/*")
public class TextAnalysisServlet extends HttpServlet {

  private static String TABLE_NAME = "wc1";
  private static final Logger logger = LogManager.getLogger(TextAnalysisServlet.class.getName());
  private final static String QUEUE_NAME = "wcQueue";
  private ObjectPool<Channel> pool;
  private WordCountDao wcd;

  /**
   * Initialize the RabbitMQ Channel pool during Servlet initialization
   *
   * @throws ServletException when Servlet can't be initialized
   */
  @Override
  public void init() throws ServletException {
    super.init();

    try {
      pool = initializePool();
    } catch (Exception e) {
      logger.info("Pool initialization failed");
    }

    wcd = new WordCountDao();
  }

  /**
   * Method to handle GET requests from client side
   *
   * @param request  http request
   * @param response http response
   * @throws IOException when PrintWriter has IO error
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    response.setContentType("text/html");
    PrintWriter out = response.getWriter();
    String urlPath = request.getPathInfo();
    String[] urlArr = urlPath.split("/");
    if (!isGetUrlValid(urlArr)) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      out.write("Parameters are not valid.");
//      out.write(String.valueOf(urlArr.length));
//      out.write("0: " + urlArr[0]);
//      out.write("1: " + urlArr[1]);
//      out.write("2: " +urlArr[2]);
//      out.write("3: " +urlArr[3]);
    } else {
      Integer wordCt = wcd.getWordCount(TABLE_NAME, urlArr[2]);
//      out.write(String.valueOf(wordCt));
      if (wordCt > 0) {
        response.setStatus(HttpServletResponse.SC_OK);
        out.write(urlArr[2] + ", count: " + wordCt);
      } else {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        out.write(urlArr[2] + " is not found in the database");
      }
    }

    response.setStatus(HttpServletResponse.SC_OK);
    JsonObject jsonResp = new JsonObject();
    jsonResp.addProperty("message", "[GET] received");
    out.write(String.valueOf(jsonResp));
    out.write("\n" + urlPath);
    out.flush();
  }

  /**
   * Method to handle a POST request It will validate the request and send messages to RabbitMQ
   * server to be processed Set response code to 404 if URL is not valid or empty, otherwise 200
   *
   * @param request  http request
   * @param response http response
   * @throws IOException when PrintWriter has IO error
   */
  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    String urlPath = request.getPathInfo();

    PrintWriter out = response.getWriter();
    // check if we have a URL!
    if (urlPath == null || urlPath.isEmpty()) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      out.write("Missing parameters.");
      return;
    }

    String[] urlArr = urlPath.split("/");
    if (!isUrlValid(urlArr)) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      out.write("Parameters are not valid.");
    } else {
      response.setStatus(HttpServletResponse.SC_OK);
      String requestBody = request.getReader().lines()
          .collect(Collectors.joining(System.lineSeparator()));
      JsonObject jsonMap = LineProcessing.processLine(requestBody);
      sendMessageToQueue(jsonMap);
      if (sendMessageToQueue(jsonMap)) {
        out.write(jsonMap.size());
      } else {
        logger.info("Failed to send message to RabbitMQ");
      }
      out.flush();
    }
  }

  /**
   * A helper method to validate URL
   *
   * @param urlPath specified URL path
   * @return true if URL path is valid, false otherwise
   */
  private boolean isUrlValid(String[] urlPath) {
    int n = urlPath.length;
    return n == 2;
  }

  /**
   * A helper method to validate URL
   *
   * @param urlPath specified URL path
   * @return true if URL path is valid, false otherwise
   */
  private boolean isGetUrlValid(String[] urlPath) {
    int n = urlPath.length;
    return n == 3;
  }

  /**
   * Method to send POST request message to RabbitMQ
   *
   * @param message in the format of JsonObject to be sent to RabbitMQ
   * @return true if message was successfully sent, false otherwise
   */
  private boolean sendMessageToQueue(JsonObject message) {
    try {
      Channel channel = pool.borrowObject();
      channel.queueDeclare(QUEUE_NAME, true, false, false, null);
      channel
          .basicPublish("", QUEUE_NAME, null, message.toString().getBytes(StandardCharsets.UTF_8));
      pool.returnObject(channel);
      return true;
    } catch (Exception e) {
      logger.info("Failed to send message to RabbitMQ");
      return false;
    }
  }

  /**
   * Method to initialize RabbitMQ Channel pool using ChannelFactory class
   *
   * @return Channel Object Pool
   * @throws Exception when initialization fails
   */
  private ObjectPool<Channel> initializePool() throws Exception {
//    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
//    config.setMinIdle(3);
//    config.setMaxIdle(5);
//    config.setMaxTotal(20);
//    ObjectPool<Channel> pool = new GenericObjectPool<>(new ChannelFactory(), config);
    return new GenericObjectPool<>(new ChannelFactory());
//    return pool;
  }
}
