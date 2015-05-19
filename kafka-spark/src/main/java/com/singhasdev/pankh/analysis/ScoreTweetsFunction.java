package com.singhasdev.pankh.analysis;

import org.apache.spark.api.java.function.*;
import scala.Tuple5;
import scala.Tuple6;

public class ScoreTweetsFunction
    implements Function<Tuple5<Long, String, String, Float, Float>,
    Tuple6<Long, String, String, Float, Float, String>>
{
  private static final long serialVersionUID = 42l;

  @Override
  public Tuple6<Long, String, String, Float, Float, String> call(
      Tuple5<Long, String, String, Float, Float> tweet)
  {
    String score;
    if (tweet._4() >= tweet._5())
      score = "positive";
    else
      score = "negative";
    return new Tuple6<Long, String, String, Float, Float, String>(
        tweet._1(),
        tweet._2(),
        tweet._3(),
        tweet._4(),
        tweet._5(),
        score);
  }
}