package org.kompany.overlord.coord;

/**
 * 
 * @author ypai
 * 
 */
public enum ReservationType {

    EXCLUSIVE {
        @Override
        public String toString() {
            return "EX";
        }

        @Override
        public String getSubCategoryPathToken() {
            return "lock";
        }
    },
    SHARED {
        @Override
        public String toString() {
            return "SH";
        }

        @Override
        public String getSubCategoryPathToken() {
            return "lock";
        }
    },
    PERMIT {
        @Override
        public String toString() {
            return "PT";
        }

        @Override
        public String getSubCategoryPathToken() {
            return "semaphore";
        }
    },
    BARRIER {
        @Override
        public String toString() {
            return "BR";
        }

        @Override
        public String getSubCategoryPathToken() {
            return "barrier";
        }
    };

    public abstract String getSubCategoryPathToken();

}
