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

import io.reign.PathScheme;
import io.reign.PathType;

/**
 * 
 * @author ypai
 * 
 */
public class CoordServicePathUtil {

    /**
     * Returns base path for lock, semaphore, etc.
     * 
     * @param pathScheme
     * @param pathContext
     * @param pathType
     * @param reservationType
     * @param entityName
     * @return
     */
    public static String getAbsolutePathEntity(PathScheme pathScheme, PathType pathType, String clusterId,
            ReservationType reservationType, String entityName) {

        return pathScheme.getAbsolutePath(pathType, clusterId, reservationType.category(), entityName);
    }

    /**
     * Returns absolute path prefix for a reservation.
     * 
     * @param pathScheme
     * @param pathContext
     * @param pathType
     * @param reservationType
     * @param entityName
     * @return
     */
    public static String getAbsolutePathReservationPrefix(PathScheme pathScheme, PathType pathType, String clusterId,
            ReservationType reservationType, String entityName) {

        return pathScheme.getAbsolutePath(pathType, clusterId, reservationType.category(), entityName, reservationType
                .prefix()
                + "_");
    }

    public static String getAbsolutePathReservationPrefix(PathScheme pathScheme, String entityPath,
            ReservationType reservationType) {
        return pathScheme.joinPaths(entityPath, reservationType.prefix() + "_");
    }
}
