package cn.gf.elasticsearch.enums;

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
}
