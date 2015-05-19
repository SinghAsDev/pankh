package com.singhasdev.pankh.analysis;

import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Set;

public class NegativeScoreFunction
    implements PairFunction<Tuple3<Long, String, String>,
    Tuple3<Long, String, String>, Float>
{
  private static final long serialVersionUID = 42l;

  @Override
  public Tuple2<Tuple3<Long, String, String>, Float> call(Tuple3<Long, String, String> tweet)
  {
    String text = tweet._3();
    Set<String> negWords = NegativeWords.getWords();
    String[] words = text.split(" ");
    int numWords = words.length;
    int numPosWords = 0;
    for (String word : words)
    {
      if (negWords.contains(word))
        numPosWords++;
    }
    return new Tuple2<Tuple3<Long, String, String>, Float>(
        new Tuple3<Long, String, String>(tweet._1(), tweet._2(), tweet._3()),
        (float) numPosWords / numWords
    );
  }
}