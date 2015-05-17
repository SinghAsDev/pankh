package com.singhasdev.pankh.analysis;

import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Set;

public class PositiveScoreFunction
    implements PairFunction<Tuple2<Long, String>,
    Tuple2<Long, String>, Float>
{
  private static final long serialVersionUID = 42l;

  @Override
  public Tuple2<Tuple2<Long, String>, Float> call(Tuple2<Long, String> tweet)
  {
    String text = tweet._2();
    Set<String> posWords = PositiveWords.getWords();
    String[] words = text.split(" ");
    int numWords = words.length;
    int numPosWords = 0;
    for (String word : words)
    {
      if (posWords.contains(word))
        numPosWords++;
    }
    return new Tuple2<Tuple2<Long, String>, Float>(
        new Tuple2<Long, String>(tweet._1(), tweet._2()),
        (float) numPosWords / numWords
    );
  }
}
