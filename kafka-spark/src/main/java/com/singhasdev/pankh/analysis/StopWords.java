package com.singhasdev.pankh.analysis;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;

public class StopWords implements Serializable
{
  private List<String> stopWords;
  private static StopWords _singleton;

  private StopWords()
  {
    this.stopWords = new ArrayList<String>();
    BufferedReader rd = null;
    try
    {
      rd = new BufferedReader(
          new InputStreamReader(
              this.getClass().getResourceAsStream("/stop-words.txt")));
      String line = null;
      while ((line = rd.readLine()) != null)
        this.stopWords.add(line);
    }
    catch (IOException ex)
    {
      Logger.getLogger(this.getClass())
          .error("IO error while initializing", ex);
    }
    finally
    {
      try {
        if (rd != null) rd.close();
      } catch (IOException ex) {}
    }
  }

  private static StopWords get()
  {
    if (_singleton == null)
      _singleton = new StopWords();
    return _singleton;
  }

  public static List<String> getWords()
  {
    return get().stopWords;
  }
}
