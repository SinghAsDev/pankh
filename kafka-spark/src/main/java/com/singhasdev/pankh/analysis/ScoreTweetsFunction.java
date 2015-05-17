package com.singhasdev.pankh.analysis;

import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple4;
import scala.Tuple5;

public class ScoreTweetsFunction
    implements Function<Tuple4<Long, String, Float, Float>,
    Tuple5<Long, String, Float, Float, String>>
{
  private static final long serialVersionUID = 42l;

  @Override
  public Tuple5<Long, String, Float, Float, String> call(
      Tuple4<Long, String, Float, Float> tweet)
  {
    String score;
    if (tweet._3() >= tweet._4())
      score = "positive";
    else
      score = "negative";
    return new Tuple5<Long, String, Float, Float, String>(
        tweet._1(),
        tweet._2(),
        tweet._3(),
        tweet._4(),
        score);
  }
}