package com.spark.application;

import scala.Tuple2;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

/**
 * Created by mali on 24.04.2016.
 */
public class PrinterFunction
        implements Function<JavaRDD<Tuple2<Long, String>>,
        Void>
{
    private static final long serialVersionUID = 42l;


    @Override
    public Void call(JavaRDD<Tuple2<Long, String>> tuple2JavaRDD) throws Exception {
        tuple2JavaRDD.foreach(new SendToSysOut());
        return null;
    }
}

class SendToSysOut implements VoidFunction<Tuple2<Long, String>>{
    private static final long serialVersionUID = 42l;

    @Override
    public void call(Tuple2<Long, String> tweet) throws Exception {
        //System.out.println(tweet);
    }
}