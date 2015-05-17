package com.singhasdev.pankh.analysis;

import java.io.IOException;

import org.apache.log4j.Logger;

import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

public class TwitterRawJsonParser
    implements PairFunction<String, Long, String>
{
  private static final long serialVersionUID = 42l;
  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public Tuple2<Long, String> call(String tweet)
  {
    try
    {
      JsonNode root = mapper.readValue(tweet, JsonNode.class);
      long id;
      String text;
      if (root.get("lang") != null &&
          "en".equals(root.get("lang").textValue()))
      {
        if (root.get("id") != null && root.get("text") != null)
        {
          id = root.get("id").longValue();
          text = root.get("text").textValue();
          text = text.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
          return new Tuple2<Long, String>(id, text);
        }
        return null;
      }
      return null;
    }
    catch (IOException ex)
    {
      Logger LOG = Logger.getLogger(this.getClass());
      LOG.error("IO error while filtering tweets", ex);
      LOG.trace(null, ex);
    }
    return null;
  }
}