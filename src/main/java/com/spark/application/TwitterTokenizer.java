package com.spark.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.IOException;
import java.text.ParseException;

/**
 * Created by mali on 29.04.2016.
 */

public class TwitterTokenizer
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
            int user_created_age;
            int statuses_count;
            float followers_ratio;
            int description_lenght=0;
            int text_lenght;
            String text;

            /*if (root.get("lang") != null && "en".equals(root.get("lang").textValue()))
            {
                if (root.get("id") != null && root.get("text") != null)
                {*/
                    JsonNode user = root.get("user");
                    user_created_age = TwitterDateParser.parseTwitterUTC(user.get("created_at").textValue());
                    statuses_count = user.get("statuses_count").intValue();
                    followers_ratio = (user.get("followers_count").floatValue() +1) / (user.get("friends_count").floatValue() + 1);
                    if (user.get("description") != null){
                        description_lenght = user.get("description").textValue().length();
                    }
                    text = root.get("text").textValue();
                    text_lenght =text.length();

                    Vector dv = Vectors.dense(text_lenght,description_lenght,followers_ratio,statuses_count,user_created_age);
                    System.out.println("Beni Bırak" +dv);
                    id = root.get("id").longValue();

                    return new Tuple2<Long, String>(id, text);
               /* }
                return null;
            }
            return null;*/



        }
        catch (IOException ex)
        {
            Logger LOG = Logger.getLogger(this.getClass());
            LOG.error("IO error while filtering tweets", ex);
            LOG.trace(null, ex);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
}



































