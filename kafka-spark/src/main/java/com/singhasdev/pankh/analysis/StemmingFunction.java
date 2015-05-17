package com.singhasdev.pankh.analysis;

import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.List;

public class StemmingFunction
    implements Function<Tuple2<Long, String>, Tuple2<Long, String>>
{
  @Override
  public Tuple2<Long, String> call(Tuple2<Long, String> tweet)
  {
    String text = tweet._2();
    List<String> stopWords = StopWords.getWords();
    for (String word : stopWords)
    {
      text = text.replaceAll("\\b" + word + "\\b", "");
    }
    return new Tuple2<Long, String>(tweet._1(), text);
  }
}
