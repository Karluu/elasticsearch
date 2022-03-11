package cn.gf.elasticsearch.enums;

import cn.hutool.core.util.StrUtil;

/**
 * Created by GuoFeng
 * Describe :
 * Created on 2020/11/30
 * Modified By :
 */
public enum CompareEnum {
    LT("<"),
    LTE("<="),
    GT(">"),
    GTE(">=");
    private String type;

    CompareEnum(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static CompareEnum getCompareEnum(String compare) {
        if (StrUtil.isEmpty(compare)) {
            return null;
        }
        CompareEnum[] values = CompareEnum.values();
        for (CompareEnum compareEnum : values) {
            if (compareEnum.type.equals(compare)) {
                return compareEnum;
            }
        }
        return null;
    }
}
