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

package io.reign.coord;

import io.reign.AbstractObserver;
import io.reign.PathScheme;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * 
 * @author ypai
 * 
 * @param <T>
 */
public abstract class CoordObserver<T> extends AbstractObserver {

    protected CoordinationServiceCache coordinationServiceCache;
    protected PathScheme pathScheme;

    void setCoordinationServiceCache(CoordinationServiceCache coordinationServiceCache) {
        this.coordinationServiceCache = coordinationServiceCache;
    }

    void setPathScheme(PathScheme pathScheme) {
        this.pathScheme = pathScheme;
    }

    public static List<String> findRevoked(List<String> updatedChildList, List<String> previousChildList,
            String entityPath, PathScheme pathScheme) {
        HashSet<String> updatedChildSet = new HashSet<String>(updatedChildList);

        Collections.sort(previousChildList);
        List<String> revoked = new ArrayList<String>(previousChildList.size());
        for (String previousChild : previousChildList) {
            if (!updatedChildSet.contains(previousChild)) {
                revoked.add(pathScheme.joinPaths(entityPath, previousChild));
            }
        }

        return revoked.size() > 0 ? revoked : Collections.EMPTY_LIST;
    }
}
