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

import io.reign.DataSerializer;
import io.reign.util.JacksonUtil;

import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * 
 * @author ypai
 * 
 * @param <T>
 */
public class MapDataSerializer<T extends Map<?, ?>> implements DataSerializer<T> {

    /**
     * Reusable Jackson JSON mapper
     */
    private static ObjectMapper OBJECT_MAPPER = JacksonUtil.getObjectMapper();

    @Override
    public byte[] serialize(T data) throws RuntimeException {
        try {
            return OBJECT_MAPPER.writeValueAsString(data).getBytes("UTF-8");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T deserialize(byte[] bytes) throws RuntimeException {
        try {
            if (bytes == null) {
                return null;
            }
            return OBJECT_MAPPER.readValue(bytes, 0, bytes.length, new TypeReference<T>() {
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
