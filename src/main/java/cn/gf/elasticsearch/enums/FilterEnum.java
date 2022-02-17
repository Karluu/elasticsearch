package cn.gf.elasticsearch.enums;

import cn.hutool.core.util.StrUtil;

/**
 * Created by GuoFeng
 * Describe :
 * Created on 2020/11/9
 * Modified By :
 */
public enum FilterEnum {
    MUST("and"),
    SHOULD("or"),
    MUST_NOT("not");

    private String filter;

    FilterEnum(String filter) {
        this.filter = filter;
    }

    public String getFilter() {
        return filter;
    }

    public static FilterEnum getFilterEnum(String word) {
        if (StrUtil.isEmpty(word)) {
            return FilterEnum.MUST;
        }
        FilterEnum[] filterEnums = FilterEnum.values();
        for (FilterEnum filterEnum : filterEnums) {
            if (word.equals(filterEnum.getFilter())) {
                return filterEnum;
            }
        }
        return FilterEnum.MUST;
    }
}
