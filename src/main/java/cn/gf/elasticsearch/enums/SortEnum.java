package cn.gf.elasticsearch.enums;

import cn.hutool.core.util.StrUtil;

/**
 * Created by GuoFeng
 * Describe :
 * Created on 2020/11/9
 * Modified By :
 */
public enum SortEnum {
    ASC("asc"),
    DESC("desc");

    private String sort;

    SortEnum(String sort) {
        this.sort = sort;
    }

    public String getSort() {
        return sort;
    }

    public static SortEnum getSortEnum(String word) {
        if (StrUtil.isEmpty(word)) {
            return SortEnum.ASC;
        }
        SortEnum[] sortEnums = SortEnum.values();
        for (SortEnum sortEnum : sortEnums) {
            if (word.equals(sortEnum.getSort())) {
                return sortEnum;
            }
        }
        return SortEnum.ASC;
    }
}
