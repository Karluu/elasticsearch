package cn.gf.elasticsearch.enums;

import cn.hutool.core.comparator.CompareUtil;
import cn.hutool.core.util.StrUtil;

/**
 * Created by GuoFeng
 * Describe :
 * Created on 2022/3/1
 * Modified By :
 */
public enum VersionEnum {
    SEVEN("7"),
    SIX("6"),
    FIVE("5");

    private String version;

    VersionEnum(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public static VersionEnum versionEnum(String version) {
        if (StrUtil.isEmpty(version)) {
            return null;
        }
        if (version.contains(".")) {
            version = version.substring(0, version.indexOf("."));
        }
        VersionEnum[] values = VersionEnum.values();
        for (VersionEnum versionEnum : values) {
            int compare = CompareUtil.compare(version, versionEnum.getVersion());
            if (0 == compare) {
                return versionEnum;
            }
        }
        return null;
    }
}
