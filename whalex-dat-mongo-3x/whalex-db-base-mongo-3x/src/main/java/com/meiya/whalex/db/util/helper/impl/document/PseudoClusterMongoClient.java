package com.meiya.whalex.db.util.helper.impl.document;

import com.mongodb.MongoClient;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * 伪集群模式
 *
 * @author 黄河森
 * @date 2020/12/25
 * @project whalex-data-driver
 */
@Slf4j
@Data
public class PseudoClusterMongoClient extends MongoClient {

    private List<MongoClient> mongoClientList;

    public PseudoClusterMongoClient() {
    }

    public PseudoClusterMongoClient(List<MongoClient> mongoClientList) {
        this.mongoClientList = mongoClientList;
    }
}
