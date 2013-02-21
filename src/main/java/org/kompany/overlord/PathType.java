package org.kompany.overlord;

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
    COORD {
        @Override
        public String toString() {
            return "coord";
        }
    },
    PRESENCE {
        @Override
        public String toString() {
            return "presence";
        }
    };

}
