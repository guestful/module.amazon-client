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

import java.io.InputStream;

/**
 * date 2014-06-20
 *
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public class AmazonS3Resource {

    private final String path;
    private final InputStream inputStream;

    public AmazonS3Resource(String path, InputStream inputStream) {
        this.path = path;
        this.inputStream = inputStream;
    }

    public String getPath() {
        return path;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    @Override
    public String toString() {
        return "https://s3.amazonaws.com/" + path;
    }
}
