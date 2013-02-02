package org.kompany.overlord.path;

/**
 * 
 * @author ypai
 * 
 */
public enum PathType {

    CONF {
        @Override
        public String toString() {
            return "conf";
        }
    },
    DATA {
        @Override
        public String toString() {
            return "data";
        }
    },
    LOCK {
        @Override
        public String toString() {
            return "lock";
        }
    },
    PRESENCE {
        @Override
        public String toString() {
            return "presence";
        }
    };

}
