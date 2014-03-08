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

package io.reign.mesg;


/**
 * 
 * @author ypai
 * 
 */
public abstract class AbstractMessage implements Message {

    private Integer id = null;

    private Object body;

    @Override
    public Integer getId() {
        return id;
    }

    @Override
    public Message setId(Integer id) {
        this.id = id;
        return this;
    }

    @Override
    public Object getBody() {
        return body;
    }

    @Override
    public Message setBody(Object body) {
        this.body = body;
        return this;
    }

}
