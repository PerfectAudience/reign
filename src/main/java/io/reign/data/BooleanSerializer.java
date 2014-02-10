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

/**
 * 
 * @author ypai
 * 
 */
public class BooleanSerializer implements DataSerializer<Boolean> {

    private static final byte TRUE = 1;
    private static final byte FALSE = 0;

    @Override
    public byte[] serialize(Boolean data) throws RuntimeException {
        return new byte[] { data ? TRUE : FALSE };
    }

    @Override
    public Boolean deserialize(byte[] bytes) throws RuntimeException {
        Byte byteObj = new Byte(bytes[0]);
        return byteObj.byteValue() == TRUE;
    }

}
