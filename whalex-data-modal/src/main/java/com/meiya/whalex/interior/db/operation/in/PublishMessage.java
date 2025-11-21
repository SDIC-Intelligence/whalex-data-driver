package com.meiya.whalex.interior.db.operation.in;

import io.swagger.annotations.ApiModel;
import lombok.Data;

@ApiModel(value = "推送消息")
@Data
public class PublishMessage {

    private Object value;
}
