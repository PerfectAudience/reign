package org.kompany.overlord.coord;

import org.kompany.overlord.PathContext;
import org.kompany.overlord.PathScheme;
import org.kompany.overlord.PathType;

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
            ReservationType reservationType, String entityName) {

        return pathScheme.getAbsolutePath(pathContext, pathType, reservationType.category(), entityName);
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
            PathType pathType, ReservationType reservationType, String entityName) {

        return pathScheme.getAbsolutePath(pathContext, pathType, reservationType.category(), entityName,
                reservationType.prefix() + "_");
    }
}
