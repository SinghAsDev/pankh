package com.singhasdev.pankh.analysis;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Set;
import java.util.HashSet;

public class PositiveWords implements Serializable
{
  public static final long serialVersionUID = 42L;
  private Set<String> posWords;
  private static PositiveWords _singleton;

  private PositiveWords()
  {
    this.posWords = new HashSet<String>();
    BufferedReader rd = null;
    try
    {
      rd = new BufferedReader(
          new InputStreamReader(
              this.getClass().getResourceAsStream("/pos-words.txt")));
      String line;
      while ((line = rd.readLine()) != null)
        this.posWords.add(line);
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

  private static PositiveWords get()
  {
    if (_singleton == null)
      _singleton = new PositiveWords();
    return _singleton;
  }

  public static Set<String> getWords()
  {
    return get().posWords;
  }
}
