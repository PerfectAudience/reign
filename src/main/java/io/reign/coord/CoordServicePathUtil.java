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
