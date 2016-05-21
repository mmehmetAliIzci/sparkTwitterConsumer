package com.spark.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.StreamingKMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.IOException;
import java.text.ParseException;

/**
 * Created by mali on 29.04.2016.
 */

public class TweetToVector
        implements Function<String, Vector>
{
    private static final long serialVersionUID = 42l;
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Vector call(String tweet)
    {
        try
        {

            JsonNode root = mapper.readValue(tweet, JsonNode.class);
            long id;
            int user_created_age;
            int statuses_count;
            float followers_ratio;
            int description_lenght=0;
            int text_lenght= 0;
            String text;

            /*if (root.get("lang") != null && "en".equals(root.get("lang").textValue()))
            {
                if (root.get("id") != null && root.get("text") != null)
                {*/
            JsonNode user = root.get("user");
            user_created_age = TwitterDateParser.parseTwitterUTC(user.get("created_at").textValue());
            statuses_count = user.get("statuses_count").intValue();
            //followers / number which user is following aka friends (spammers do this alot) -> Bigger the spammer lesser the value
            followers_ratio = (user.get("followers_count").floatValue() +1) / (user.get("friends_count").floatValue() + 1);
            try{
                description_lenght = user.get("description").textValue().length();
            }catch (NullPointerException e){
              //  System.out.println("Exception !" + user.toString());
            }
            text = root.get("text").textValue();
            //System.out.println(text);

            text_lenght =text.length();

            String[] result = text.split("\\s");
            int hashtag_count = 0;
            for (int x=0; x<result.length; x++){
                if (result[x].startsWith("#")){
                    hashtag_count++;
                }
            }
            //System.out.println("Hashtag count of the text is "+hashtag_count);

            //System.out.println("----------------------");

            // recently created users = false if Twitter users who's profile was created less than a day ago
            int user_created_age_result = (user_created_age > 1) ? 1 : 0;

            // Users That Create Little Content =  false if users who have only ever created less than 50 tweets
            int statuses_count_result = (statuses_count > 50) ? 1 : 0;

            // false if Users With Few Followers and big friends
            int followers_ratio_result = (followers_ratio > 0.01)? 1 : 0;

            // false Users With Short Descriptions
            int description_length_result = (description_lenght >20 )? 1 : 0;

            // false if messages with a Large Numbers Of HashTags
            int hashtag_count_result = (hashtag_count < 4) ? 1: 0;

            // false if Messages with Short Content Length
            int text_lenght_result = (text_lenght > 0)? 1 : 0;


            Vector dv = Vectors.dense(user_created_age_result,statuses_count_result,followers_ratio_result,description_length_result,text_lenght_result,hashtag_count_result);

/*            System.out.println("Vector is  :- ");
            for (int i = 0; i<dv.size(); i++) {
                System.out.println("Number = " + dv.toArray()[i]);
            }*/
            System.out.println(text);

            System.out.println("--------------------------------------------------------");
            //System.out.println("Beni Bırak" +dv);
            id = root.get("id").longValue();



            return dv;
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



































