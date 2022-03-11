package cn.gf.elasticsearch.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by GuoFeng
 * Describe :
 * Created on 2022/3/1
 * Modified By :
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class VersionDTO {

    private String version;

    private String nodeName;
}
