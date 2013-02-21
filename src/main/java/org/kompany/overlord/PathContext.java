package org.kompany.overlord;

/**
 * 
 * @author ypai
 * 
 */
public enum PathContext {

    INTERNAL {
        @Override
        public String toString() {
            return "internal";
        }
    },
    USER {
        @Override
        public String toString() {
            return "user";
        }
    };

}
