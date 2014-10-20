/**
 * Copyright (C) 2013 Guestful (info@guestful.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.guestful.client.amazon;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.Locale;
import java.util.SimpleTimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public class AmazonS3Client {

    private static final Logger LOGGER = Logger.getLogger(AmazonS3Client.class.getName());
    private static final Pattern ENCODED_CHARACTERS_PATTERN = Pattern.compile(Pattern.quote("+") + "|" + Pattern.quote("*") + "|" + Pattern.quote("%7E") + "|" + Pattern.quote("%2F"));

    private final Client client;
    private final WebTarget target;
    private boolean enabled = true;
    private final String accessKey;
    private final String secretKey;

    public AmazonS3Client(String accessKey, String secretKey) {
        this(ClientBuilder.newClient(), accessKey, secretKey);
    }

    public AmazonS3Client(Client restClient, String accessKey, String secretKey) {
        this.client = restClient;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.target = buildWebTarget();
    }

    public Client getClient() {
        return client;
    }

    protected WebTarget buildWebTarget() {
        return getClient().target("http://s3.amazonaws.com");
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public AmazonS3Bucket getBucket(String name) {
        return new AmazonS3Bucket(this, name);
    }

    Response request(String method, String path, Entity<?> entity) {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest(method + " " + path);
        }
        if (!path.startsWith("/")) path = "/" + path;
        Date date = new Date();
        return isEnabled() ?
            target
                .path(path)
                .request()
                .header(HttpHeaders.DATE, date)
                .header(HttpHeaders.AUTHORIZATION, "AWS " + getAccessKey() + ":" + signRequest(
                    method,
                    path,
                    entity == null ? null : entity.getMediaType(),
                    date))
                .method(method, entity) :
            Response.ok().build();
    }

    private String signRequest(String method, String path, MediaType mediaType, Date date) {
        SimpleDateFormat rfc822DateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
        rfc822DateFormat.setTimeZone(new SimpleTimeZone(0, "GMT"));
        String data = method + "\n\n" +
            (mediaType == null ? "" : mediaType.toString()) + "\n" +
            rfc822DateFormat.format(date) + "\n" +
            urlEncode(path, true);
        Mac mac;
        try {
            mac = Mac.getInstance("HmacSHA1");
            mac.init(new SecretKeySpec(getSecretKey().getBytes(StandardCharsets.UTF_8), "HmacSHA1"));
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new AmazonS3Exception(e);
        }
        return Base64.getEncoder().encodeToString(mac.doFinal(data.getBytes(StandardCharsets.UTF_8)));
    }

    static String urlEncode(final String value, final boolean path) {
        if (value == null) {
            return "";
        }
        try {
            String encoded = URLEncoder.encode(value, "UTF-8");
            Matcher matcher = ENCODED_CHARACTERS_PATTERN.matcher(encoded);
            StringBuffer buffer = new StringBuffer(encoded.length());
            while (matcher.find()) {
                String replacement = matcher.group(0);
                if ("+".equals(replacement)) {
                    replacement = "%20";
                } else if ("*".equals(replacement)) {
                    replacement = "%2A";
                } else if ("%7E".equals(replacement)) {
                    replacement = "~";
                } else if (path && "%2F".equals(replacement)) {
                    replacement = "/";
                }
                matcher.appendReplacement(buffer, replacement);
            }
            matcher.appendTail(buffer);
            return buffer.toString();
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }

}
