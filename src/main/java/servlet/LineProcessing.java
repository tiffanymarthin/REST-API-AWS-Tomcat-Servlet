package servlet;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map;

/**
 * This static class is responsible to process each text line
 */
public class LineProcessing {

  /**
   * Static method to convert a line of text to tuples of word counts
   * @param line a line of text
   * @return JsonObject of word count tuples
   */
  public static JsonObject processLine(String line) {
    String[] arr = line.trim().split("\\s+");
    Map<String, Integer> wordCount = new HashMap<>();
    for (String word : arr) {
      wordCount.put(word, wordCount.getOrDefault(word, 0) + 1);
    }

    Gson gson = new Gson();
    return gson.toJsonTree(wordCount).getAsJsonObject();
  }

//  public static void main(String[] args) {
//    String line = "   Thread Pools 47";
//    JsonObject result = processLine(line);
//    String after = result.getAsString();
//    System.out.println(result);
//    System.out.println(result.size());
//    System.out.println(after);
//  }
}
