package com.singhasdev.pankh.analysis;

import java.io.IOException;

import org.apache.log4j.Logger;

import org.apache.spark.api.java.function.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import scala.Tuple3;

public class TwitterRawJsonParser
    implements Function<String, Tuple3<Long, String, String>>
{
  private static final long serialVersionUID = 42l;
  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public Tuple3<Long, String, String> call(String tweet)
  {
    try
    {
      JsonNode root = mapper.readValue(tweet, JsonNode.class);
      long id;
      String text;
      final JsonNode lang = root.get("lang");
      if (lang != null && "en".equals(lang.textValue()))
      {
        if (root.get("id") != null && root.get("text") != null)
        {
          id = root.get("id").longValue();
          text = root.get("text").textValue();
          String modifiedText = text.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
          return new Tuple3<Long, String, String>(id, text, modifiedText);
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