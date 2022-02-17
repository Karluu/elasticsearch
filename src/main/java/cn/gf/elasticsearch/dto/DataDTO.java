package cn.gf.elasticsearch.dto;

import cn.hutool.json.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by GuoFeng
 * Describe :
 * Created on 2022/1/13
 * Modified By :
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DataDTO {

    private String index;
    private String type;
    private String id;
    private JSONObject data;
}
