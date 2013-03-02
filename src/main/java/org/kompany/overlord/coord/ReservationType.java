package org.kompany.overlord.coord;

/**
 * 
 * @author ypai
 * 
 */
public enum ReservationType {

    LOCK_EXCLUSIVE {
        @Override
        public String prefix() {
            return "EX";
        }

        @Override
        public String category() {
            return "lock";
        }
    },
    LOCK_SHARED {
        @Override
        public String prefix() {
            return "SH";
        }

        @Override
        public String category() {
            return "lock";
        }
    },
    SEMAPHORE {
        @Override
        public String prefix() {
            return "PT";
        }

        @Override
        public String category() {
            return "semaphore";
        }
    },
    BARRIER {
        @Override
        public String prefix() {
            return "BR";
        }

        @Override
        public String category() {
            return "barrier";
        }
    };

    public abstract String category();

    public abstract String prefix();

}