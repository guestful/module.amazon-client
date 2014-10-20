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

import javax.activation.FileTypeMap;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Collection;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public class AmazonS3Bucket {

    private final AmazonS3Client client;
    private final String name;

    public AmazonS3Bucket(AmazonS3Client client, String name) {
        this.client = client;
        if (name.startsWith("/")) name = name.substring(1);
        if (name.endsWith("/")) name = name.substring(0, name.length() - 1);
        this.name = name;
    }

    public AmazonS3Client getClient() {
        return client;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }

    public AmazonS3Resource getFirst(Collection<String> paths) throws FileNotFoundException {
        for (String path : paths) {
            try {
                return get(path);
            } catch (FileNotFoundException e) {
                // ignore
            }
        }
        throw new FileNotFoundException(String.join(", ", paths));
    }

    public AmazonS3Resource get(String path) throws FileNotFoundException {
        if (path == null) throw new NullPointerException();
        if (path.startsWith("/")) path = path.substring(1);
        String fullPath = getName() + "/" + path;
        Response response = getClient().request(HttpMethod.GET, fullPath, null);
        if (response.getStatus() == 404) {
            throw new FileNotFoundException(fullPath);
        }
        if (response.getStatus() != 200) {
            throw new AmazonS3Exception("Unable get " + path + " in bucket " + getName() + ": " + response.getStatus() + " - " + response.readEntity(String.class));
        }
        return new AmazonS3Resource(fullPath, response.readEntity(InputStream.class));
    }

    public String put(String path, InputStream data) {
        if (path == null || data == null) throw new NullPointerException();
        if (path.startsWith("/")) path = path.substring(1);
        String contentType = FileTypeMap.getDefaultFileTypeMap().getContentType(path);
        String fullPath = getName() + "/" + path;
        Response response = getClient().request(
            HttpMethod.PUT,
            fullPath,
            Entity.entity((StreamingOutput) output -> {
                byte[] buffer = new byte[8192];
                int c;
                try {
                    while ((c = data.read(buffer)) != -1) {
                        output.write(buffer, 0, c);
                    }
                } finally {
                    data.close();
                    output.close();
                }
            }, contentType)
        );
        try {
            if (response.getStatus() != 200) {
                throw new AmazonS3Exception("Unable to store data at " + path + " in bucket " + getName() + ": " + response.getStatus() + " - " + response.readEntity(String.class));
            }
        } finally {
            response.close();
        }
        return "https://s3.amazonaws.com/" + fullPath;
    }

}

