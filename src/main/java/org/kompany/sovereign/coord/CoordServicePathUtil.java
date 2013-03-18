package org.kompany.sovereign.coord;

import org.kompany.sovereign.PathContext;
import org.kompany.sovereign.PathScheme;
import org.kompany.sovereign.PathType;

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
    public static String getAbsolutePathEntity(PathScheme pathScheme, PathContext pathContext, PathType pathType,
            String clusterId, ReservationType reservationType, String entityName) {

        return pathScheme.getAbsolutePath(pathContext, pathType, clusterId, reservationType.category(), entityName);
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
    public static String getAbsolutePathReservationPrefix(PathScheme pathScheme, PathContext pathContext,
            PathType pathType, String clusterId, ReservationType reservationType, String entityName) {

        return pathScheme.getAbsolutePath(pathContext, pathType, clusterId, reservationType.category(), entityName,
                reservationType.prefix() + "_");
    }

    public static String getAbsolutePathReservationPrefix(PathScheme pathScheme, String entityPath,
            ReservationType reservationType) {
        return pathScheme.join(entityPath, reservationType.prefix() + "_");
    }
}
