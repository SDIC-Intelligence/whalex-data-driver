package com.meiya.whalex.db.entity.stream;

import com.meiya.whalex.db.kafka.admin.client.MyKafkaAdminClient;
import com.meiya.whalex.db.util.helper.impl.stream.BaseKafkaConfigHelper;
import com.meiya.whalex.db.util.param.impl.stream.KafkaConstantsProperty;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * kafka 连接对象实体
 * 采用懒加载的方式进行创建
 */
@Slf4j
public class KafkaClient implements AutoCloseable {

    /**
     * admin 客户端
     */
    private volatile AdminClient adminClient;

    /**
     * 自定义 admin 客户端，对 kafka-client 1.x 版本的客户端做补充
     */
    private volatile MyKafkaAdminClient myKafkaAdminClient;

    /**
     * 生产者
     */
    private volatile KafkaProducer producer;
    /**
     * 消费者
     */
    private volatile KafkaConsumer consumer;

    /**
     * kafka 配置信息
     */
    private KafkaDatabaseInfo databaseConf;

    /**
     * 工具类对象
     */
    private BaseKafkaConfigHelper helper;

    public AdminClient getAdminClient() {
        if (adminClient == null) {
            synchronized (KafkaClient.class) {
                if (adminClient == null) {
                    this.adminClient = createAdmin();
                }
            }
        }
        return adminClient;
    }

    /**
     * 该 MyKafkaAdminClient 对象为 对 kafka-client 1.x 版本 自定义补充的一些Admin 操作方法
     * 只有在 kafka-client 为 1.x 版本中才需要使用
     * kafka-client 2.x 版本中可以通过上面的 AdminClient 直接操作
     * @return
     */
    public MyKafkaAdminClient getMyKafkaAdminClient_V1() {
        if (myKafkaAdminClient == null) {
            synchronized (KafkaClient.class) {
                if (myKafkaAdminClient == null) {
                    this.myKafkaAdminClient = createMyKafkaAdminInVer1_x();
                }
            }
        }
        return myKafkaAdminClient;
    }

    public boolean isExistAdminClient() {
        return adminClient != null;
    }

    public KafkaProducer getProducer() {
        if (producer == null) {
            synchronized (KafkaClient.class) {
                if (producer == null) {
                    this.producer = createProducer();
                }
            }
        }
        return producer;
    }

    public boolean isExistProducer() {
        return producer != null;
    }

    public KafkaConsumer getConsumer() {
        if (consumer == null) {
            synchronized (KafkaClient.class) {
                if (consumer == null) {
                    this.consumer = createConsumer();
                }
            }
        }
        return consumer;
    }

    public boolean isExistConsumer() {
        return consumer != null;
    }

    public KafkaDatabaseInfo getDatabaseConf() {
        return databaseConf;
    }

    public void setDatabaseConf(KafkaDatabaseInfo databaseConf) {
        this.databaseConf = databaseConf;
    }

    public BaseKafkaConfigHelper getHelper() {
        return helper;
    }

    public void setHelper(BaseKafkaConfigHelper helper) {
        this.helper = helper;
    }

    public KafkaClient() {
    }

    public KafkaClient(KafkaDatabaseInfo databaseConf, BaseKafkaConfigHelper helper) {
        this.databaseConf = databaseConf;
        this.helper = helper;
    }

    /**
     * 创建生产者
     */
    public KafkaProducer createProducer() {
        return this.helper.createProducer(this.databaseConf);
    }

    /**
     * 创建消费者
     */
    public KafkaConsumer createConsumer() {
        return helper.createConsumer(this.databaseConf);
    }

    /**
     * 创建管理者
     *
     * @return
     */
    public AdminClient createAdmin() {
        return helper.createAdmin(this.databaseConf);
    }

    /**
     * 该 MyKafkaAdminClient 对象为 对 kafka-client 1.x 版本 自定义补充的一些Admin 操作方法
     * 只有在 kafka-client 为 1.x 版本中才需要使用
     * kafka-client 2.x 版本中可以通过上面的 AdminClient 直接操作
     * @return
     */
    public MyKafkaAdminClient createMyKafkaAdminInVer1_x() {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", databaseConf.getServiceUrl());
        helper.authKafka(this.databaseConf, adminProps);
        return MyKafkaAdminClient.createDatKafkaAdmin(adminProps);
    }

    /**
     * 关闭资源
     */
    @Override
    public void close() {
        if (this.producer != null) {
            this.producer.flush();
            this.producer.close();
        }
        if (this.consumer != null) {
            this.consumer.close();
        }
        if (this.adminClient != null) {
            this.adminClient.close();
        }
        if (this.myKafkaAdminClient != null) {
            this.myKafkaAdminClient.close();
        }
    }
}
