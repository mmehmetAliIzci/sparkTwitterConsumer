package com.spark.application;

import org.apache.spark.mllib.clustering.StreamingKMeans;
import org.apache.spark.mllib.linalg.Vector;

import java.util.TimerTask;

/**
 * Created by mali on 01.05.2016.
 */
public class ModelPrinterFunction extends TimerTask {
    @Override
    public void run() {
        StreamingKMeans model = Application.model;

        Vector vec1 = model.model().clusterCenters()[0];
        Vector vec2 = model.model().clusterCenters()[1];


        // let us print all the elements available in vector
        System.out.println("Centers 1 :- ");
        for (int i = 0; i<vec1.size(); i++) {
            System.out.println("Number = " + vec1.toArray()[i]);
        }
        System.out.println("Centers 2 :- ");
        for (int i = 0; i<vec2.size(); i++) {
            System.out.println("Number = " + vec2.toArray()[i]);
        }

        System.out.println("--------------------------------------------------------");
    }
}
