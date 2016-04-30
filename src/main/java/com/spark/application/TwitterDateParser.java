package com.spark.application;

/**
 * Created by mali on 29.04.2016.
 */
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;


/** Parse UTC time from Twitter's created_at field.
 *
 * Compile and run: javac TwitterDateParser.java && java TwitterDateParser
 *
 * See:
 * - http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
 * - http://docs.oracle.com/javase/7/docs/api/java/text/DateFormat.html
 * - http://docs.oracle.com/javase/7/docs/api/java/util/Locale.html
 * - https://dev.twitter.com/docs/platform-objects/tweets
 */
public class TwitterDateParser {
    public static int parseTwitterUTC(String date)
            throws ParseException {

        String twitterFormat="EEE MMM dd HH:mm:ss ZZZZZ yyyy";

        SimpleDateFormat sf = new SimpleDateFormat(twitterFormat, Locale.ENGLISH);
        sf.setLenient(true);
        long tweet = sf.parse(date).getTime();
        long now = System.currentTimeMillis();

        return daysBetween(tweet,now);
    }

    public static int daysBetween(long d1, long d2){
        return (int)( (d2- d1) / (1000 * 60 * 60 * 24));
    }

}