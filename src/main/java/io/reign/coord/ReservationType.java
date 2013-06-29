package io.reign.coord;

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

        @Override
        public boolean isExclusive() {
            return true;
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

        @Override
        public boolean isExclusive() {
            return false;
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

        @Override
        public boolean isExclusive() {
            return false;
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

        @Override
        public boolean isExclusive() {
            return false;
        }
    };

    // public static final Map<String, ReservationType> prefixMap = new HashMap<String, ReservationType>();
    // static {
    // prefixMap.put(LOCK_EXCLUSIVE.prefix(), LOCK_EXCLUSIVE);
    // }

    public abstract String category();

    public abstract String prefix();

    public abstract boolean isExclusive();

    /**
     * 
     * @param prefixedString
     * @return
     */
    public static boolean isExclusive(String prefixedString) {
        for (ReservationType rt : ReservationType.values()) {
            if (prefixedString.startsWith(rt.prefix())) {
                return rt.isExclusive();
            }
        }
        throw new IllegalArgumentException("Unrecognized ReservationType prefix:  " + prefixedString);
    }

    /**
     * 
     * @param category
     * @return
     */
    public static ReservationType fromCategory(String category) {
        for (ReservationType rt : ReservationType.values()) {
            if (rt.category().equals(category)) {
                return rt;
            }
        }
        return null;
    }

}
