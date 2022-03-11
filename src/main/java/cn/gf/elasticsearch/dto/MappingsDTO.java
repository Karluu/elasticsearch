package cn.gf.elasticsearch.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

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
public class MappingsDTO {

    private String type;

    private String field;

    private String fieldType;

    private List<MappingsDTO> nest;
}
