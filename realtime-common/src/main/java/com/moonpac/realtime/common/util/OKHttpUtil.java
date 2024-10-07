package com.moonpac.realtime.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.moonpac.realtime.common.bean.vcenter.HorizonParam;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import javax.net.ssl.*;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import com.moonpac.realtime.common.bean.vcenter.VcenterParam;

@Slf4j
public class OKHttpUtil {

    public static void main(String[] args) throws Exception {
        String ip = getIP("http://exap.com");
        System.out.println(ip);
    }

    public static String getIP(String url) throws Exception {
        //创建URL对象
        URL uRL = new URL(url);
        return uRL.getHost();
    }

    public static String vcRequest(OkHttpClient okHttpClient, String url, String token, Map<String, String> params) throws Exception{

        HttpUrl.Builder httpBuilder = HttpUrl.parse(url).newBuilder();

        if (params != null && !params.isEmpty()) {
            for (Map.Entry<String, String> param : params.entrySet()) {
                httpBuilder.addQueryParameter(param.getKey(), param.getValue());
            }
        }
        Request request = new Request.Builder()// 构造一个 Request 对象
                .get()// 标识为 GET 请求
                .url(httpBuilder.build()) // 设置请求路径
                .addHeader("vmware-api-session-id", token)// 添加头信息
                .build();
        Response response = okHttpClient.newCall(request).execute();
        return response.body().string();

    }

    public static String csRequest(OkHttpClient okHttpClient, String url, String token, Map<String, String> params) throws Exception {

        HttpUrl.Builder httpBuilder = HttpUrl.parse(url).newBuilder();

        if (params != null && !params.isEmpty()) {
            for (Map.Entry<String, String> param : params.entrySet()) {
                httpBuilder.addQueryParameter(param.getKey(), param.getValue());
            }
        }
        Request request = new Request.Builder()// 构造一个 Request 对象
                .get()// 标识为 GET 请求
                .url(httpBuilder.build()) // 设置请求路径
                .addHeader("Authorization", "Bearer "+token)// 添加头信息
                .build();
        Response response = okHttpClient.newCall(request).execute();
        return response.body().string();

    }


    public static String getVCToken(OkHttpClient okHttpClient, VcenterParam vcenterParam) throws IOException {

        String tokenEndpoint = vcenterParam.getUrl() + "/api/session";

        String base64Credentials = Credentials.basic(vcenterParam.getUsername(), vcenterParam.getPassword());

        Request request = new Request.Builder()
                .url(tokenEndpoint)
                .header("Authorization", base64Credentials)
                .post(RequestBody.create("".getBytes(), MediaType.parse("application/json")))
                .build();

        Response response = null;
        String token = null;
        response = okHttpClient.newCall(request).execute();
        ResponseBody responseBody = response.body();
        if (responseBody != null) {
            token = responseBody.string().replace("\"", "");
        }
        return token;
    }

    public static String getCSToken(OkHttpClient okHttpClient, HorizonParam csParam) throws Exception {

        String tokenEndpoint = "https://" + csParam.getHorizonIp() + "/rest/login";

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("username", csParam.getUsername());
        jsonObject.put("password", csParam.getPassword());
        jsonObject.put("domain", csParam.getDomain());

        RequestBody requestBody = RequestBody.create(jsonObject.toJSONString().getBytes(StandardCharsets.UTF_8),
                MediaType.parse("application/json"));

        Request request = new Request.Builder()
                .url(tokenEndpoint)
                .post(requestBody)
                .build();

        Response response = okHttpClient.newCall(request).execute();
        if (response.isSuccessful()) {
            String responseBody = response.body().string();
            // 处理响应
            JSONObject json = JSON.parseObject(responseBody);
            return json.getString("access_token");
        } else {
            log.error("获取CSToken异常，url：{}，错误码：{}", tokenEndpoint, response.code());
            return null;
        }

    }

    public static OkHttpClient getOKHttpClient() throws Exception {

        // 创建连接池对象
        // 可以使用连接池来管理和复用HTTP和HTTPS连接，以提高性能和效率
        ConnectionPool connectionPool =
                new ConnectionPool(5, 10, TimeUnit.MINUTES);

        OkHttpClient client = new OkHttpClient().newBuilder()
                .sslSocketFactory(getIgnoreInitedSslContext().getSocketFactory(),IGNORE_SSL_TRUST_MANAGER_X509)
                .hostnameVerifier(getIgnoreSslHostnameVerifier())
                .connectionPool(connectionPool)
                .callTimeout(10, TimeUnit.SECONDS)
                .connectTimeout(10,TimeUnit.SECONDS)
                .readTimeout(10,TimeUnit.SECONDS)
                .build();

        return client;
    }

    private static final X509TrustManager IGNORE_SSL_TRUST_MANAGER_X509 = new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[] {};
        }
    };

    /**
     * Get initialized SSLContext instance which ignored SSL certification
     *
     * @return
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     */
    private static SSLContext getIgnoreInitedSslContext() throws NoSuchAlgorithmException, KeyManagementException {
        SSLContext ssl = SSLContext.getInstance("SSL");
        ssl.init(null, new TrustManager[] { IGNORE_SSL_TRUST_MANAGER_X509 }, new SecureRandom());
        return ssl;
    }

    /**
     * Get HostnameVerifier which ignored SSL certification
     *
     * @return
     */
    private static HostnameVerifier getIgnoreSslHostnameVerifier() {
        return new HostnameVerifier() {
            @Override
            public boolean verify(String arg0, SSLSession arg1) {
                return true;
            }
        };
    }

}
