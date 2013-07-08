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
