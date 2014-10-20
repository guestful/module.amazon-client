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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.LogManager;

import static org.junit.Assert.*;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 * @date 2014-03-30
 */
@RunWith(JUnit4.class)
public class AmazonS3ClientTest {

    @Test
    public void test() throws IOException {
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.install();
        LoggerFactory.getILoggerFactory();

        RandomAccessFile aFile = new RandomAccessFile("src/test/data/resto pic.jpg", "r");
        FileChannel inChannel = aFile.getChannel();
        long fileSize = inChannel.size();
        ByteBuffer buffer = ByteBuffer.allocate((int) fileSize);
        inChannel.read(buffer);
        inChannel.close();

        Client restClient = ClientBuilder.newBuilder().build();

        String AWS_ACCESS_KEY_ID = System.getenv("AWS_ACCESS_KEY_ID");
        String AWS_SECRET_KEY = System.getenv("AWS_SECRET_KEY");

        String uri = "/pictures/restaurants/10Gea1uJJNEQGV-l0dd-nw/GUESTFUL/test/toto " + System.currentTimeMillis() + ".jpg";
        AmazonS3Client client = new AmazonS3Client(restClient, AWS_ACCESS_KEY_ID, AWS_SECRET_KEY);
        AmazonS3Bucket guestful = client.getBucket("guestful");
        String path = guestful.put(uri, new ByteArrayInputStream(buffer.array()));
        System.out.println(path);

        InputStream is = guestful.get(uri).getInputStream();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int len;
        byte[] buff = new byte[8096];
        while ((len = is.read(buff)) != -1) {
            baos.write(buff, 0, len);
        }
        is.close();
        assertEquals(fileSize, baos.size());
        assertArrayEquals(buffer.array(), baos.toByteArray());

        try {
            guestful.get(uri + "inexisting");
            fail();
        } catch (FileNotFoundException e) {
            assertTrue(e.getMessage().startsWith("guestful/pictures/restaurants/10Gea1uJJNEQGV-l0dd-nw/GUESTFUL/test/toto "));
        }

        new AmazonS3Client(restClient, AWS_ACCESS_KEY_ID, AWS_SECRET_KEY)
            .getBucket("guestful-templates")
            .get("development/sms/GUESTFUL/SmsToStaffOnReservationCancelation.txt");

    }

}
