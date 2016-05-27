package com.spark.application;


import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mali on 26.05.2016.
 */
public class HttpNotifier {
    public static void main(String[] args) throws IOException {

/*        HttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost("http://192.168.1.140:3000");
        String content = String.format(
                "{\"id\": \"%d\", "     +
                        "\"text\": \"%s\", "    +
                        "\"pos\": \"%f\", "     +
                        "\"neg\": \"%f\", "     +
                        "\"score\": \"%s\" }",
                tweet._1(),
                tweet._2(),
                tweet._3(),
                tweet._4(),
                tweet._5());

        try
        {
            post.setEntity(new StringEntity(content));
            HttpResponse response = client.execute(post);
            org.apache.http.util.EntityUtils.consume(response.getEntity());
        }
        catch (Exception ex)
        {
            Logger LOG = Logger.getLogger(this.getClass());
            LOG.error("exception thrown while attempting to post", ex);
            LOG.trace(null, ex);
        }*/
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost("http://192.168.1.140:3000/post");
        List<NameValuePair> nvps = new ArrayList<NameValuePair>();
        nvps.add(new BasicNameValuePair("username", "vip"));
        nvps.add(new BasicNameValuePair("password", "secret"));
        httpPost.setEntity(new UrlEncodedFormEntity(nvps));
        CloseableHttpResponse response2 = httpclient.execute(httpPost);

        try {
            System.out.println(response2.getStatusLine());
            HttpEntity entity2 = response2.getEntity();
            // do something useful with the response body
            // and ensure it is fully consumed
            EntityUtils.consume(entity2);
        } finally {
            response2.close();
        }
    }
}
