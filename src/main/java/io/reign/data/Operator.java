package io.reign.data;

import java.util.List;

/**
 * 
 * @author ypai
 * 
 */
public enum Operator {

    SUM {
        @Override
        public <T> T apply(List<T> args) {
            T result = null;
            return result;
        }
    },
    MIN {
        @Override
        public <T> T apply(List<T> args) {
            T result = null;
            return result;
        }
    },
    MAX {
        @Override
        public <T> T apply(List<T> args) {
            T result = null;
            return result;
        }
    },
    AVG {
        @Override
        public <T> T apply(List<T> args) {
            T result = null;
            return result;
        }
    },
    OR {
        @Override
        public <T> T apply(List<T> args) {
            T result = null;
            return result;
        }
    },
    AND {
        @Override
        public <T> T apply(List<T> args) {
            T result = null;
            return result;
        }
    };

    public abstract <T> T apply(List<T> args);
}
