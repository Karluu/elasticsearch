package cn.gf.elasticsearch.enums;

import org.elasticsearch.common.unit.TimeValue;

/**
 * Created by GuoFeng
 * Describe :
 * Created on 2022/1/13
 * Modified By :
 */
public enum TimeUnit {
    /**
     * nanos time
     */
    N {
        @Override
        public TimeValue getTime(long timeValue) {
            return TimeValue.timeValueNanos(validate(timeValue));
        }

        @Override
        public String getSuffix() {
            return "nanos";
        }
    },
    /**
     * millis time
     */
    MI {
        @Override
        public TimeValue getTime(long timeValue) {
            return TimeValue.timeValueMillis(validate(timeValue));
        }

        @Override
        public String getSuffix() {
            return "ms";
        }
    },
    /**
     * seconds time
     */
    S {
        @Override
        public TimeValue getTime(long timeValue) {
            return TimeValue.timeValueSeconds(validate(timeValue));
        }

        @Override
        public String getSuffix() {
            return "s";
        }
    },
    /**
     * minutes time
     */
    M {
        @Override
        public TimeValue getTime(long timeValue) {
            return TimeValue.timeValueMinutes(validate(timeValue));
        }

        @Override
        public String getSuffix() {
            return "m";
        }
    },
    /**
     * hour time
     */
    H {
        @Override
        public TimeValue getTime(long timeValue) {
            return TimeValue.timeValueHours(validate(timeValue));
        }

        @Override
        public String getSuffix() {
            return "h";
        }
    };

    public long validate(long timeValue) {
        return timeValue <= Integer.MAX_VALUE ? timeValue : 0L;
    }

    public abstract TimeValue getTime(long timeValue);

    public abstract String getSuffix();
}
