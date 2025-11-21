package com.meiya.whalex.interior.db.operation.in;

import io.swagger.annotations.ApiModel;
import lombok.Data;

import java.util.function.Consumer;

@ApiModel(value = "订阅消息")
@Data
public class SubscribeMessage {

    private boolean async;

    private Consumer<Object> consumer;
}
