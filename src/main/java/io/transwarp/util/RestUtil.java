package io.transwarp.util;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by zouhao on 16/1/11.
 *
 * @ClassName: ${ClassName}
 * @Description: TODO
 * @author: zouhao
 * @date: 16/1/11
 */
public class RestUtil {
    private static RestUtil restUtil = new RestUtil();
    private Logger logger = Logger.getLogger(this.getClass());

    public RestUtil() {
        logger.info("Loading " + this.getClass().getName() + " constructor successfully");
    }

    public static void main(String[] args) {
        try {
            RestUtil restUtil = new RestUtil();
            String resultString = restUtil.doGet("http://172.16.2.203:4040/api/jobs?status=succeeded");
            System.out.println("json: " + resultString);
        } catch (Exception e) {
            restUtil.logger.error(e.getMessage(), e);
        }
    }

    public String doPost(String url, String args) throws Exception {
        URL restURL = new URL(url);
        HttpURLConnection conn = (HttpURLConnection) restURL.openConnection();
        /**
         * https://stackoverflow.com/questions/18687122/java-io-ioexception-server-returned-http-response-code-405-for-url
         * POST request
         */
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setAllowUserInteraction(false);
        PrintStream ps = new PrintStream(conn.getOutputStream());
        ps.print(args);
        ps.close();
        BufferedReader bReader = new BufferedReader(new InputStreamReader(
                conn.getInputStream()));
        String line, resultStr = "";
        while (null != (line = bReader.readLine())) {
            resultStr += line;
        }
        bReader.close();
        return resultStr;
    }

    public String doGet(String url) throws Exception {
        URL restURL = new URL(url);
        HttpURLConnection conn = (HttpURLConnection) restURL.openConnection();
        BufferedReader bReader = new BufferedReader(new InputStreamReader(
                conn.getInputStream()));
        String line, resultStr = "";
        while (null != (line = bReader.readLine())) {
            resultStr += line;
        }
        bReader.close();
        return resultStr;
    }
}