/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package io.reign.data;

import java.io.UnsupportedEncodingException;

import io.reign.DataSerializer;

/**
 * Pass-through serializer for byte arrays.
 * 
 * @author ypai
 * 
 */
public class Utf8StringSerializer implements DataSerializer<String> {

    @Override
    public byte[] serialize(String data) throws RuntimeException {
        try {
            return data.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String deserialize(byte[] bytes) throws RuntimeException {
        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

}
