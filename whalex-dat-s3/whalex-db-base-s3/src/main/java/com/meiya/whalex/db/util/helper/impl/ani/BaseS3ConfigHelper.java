package com.meiya.whalex.db.util.helper.impl.ani;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.meiya.whalex.business.entity.DatabaseConf;
import com.meiya.whalex.business.entity.TableConf;
import com.meiya.whalex.db.entity.AbstractCursorCache;
import com.meiya.whalex.db.entity.ani.S3BucketInfo;
import com.meiya.whalex.db.entity.ani.S3DatabaseInfo;
import com.meiya.whalex.db.util.helper.AbstractDbModuleConfigHelper;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * S3 配置管理工具
 *
 * @author xult
 * @date 2020/3/27
 * @project whale-cloud-platformX
 */
@Slf4j
public class BaseS3ConfigHelper extends AbstractDbModuleConfigHelper<AmazonS3, S3DatabaseInfo, S3BucketInfo, AbstractCursorCache> {
    @Override
    public void init(boolean loadDbConf) {
        super.init(loadDbConf);
    }

    @Override
    public boolean checkDataSourceStatus(AmazonS3 connect) {
        //todo
        return true;
    }

    @Override
    public S3DatabaseInfo initDbModuleConfig(DatabaseConf conf) {
        String connSetting = conf.getConnSetting();
        S3DatabaseInfo s3DatabaseInfo = JsonUtil.jsonStrToObject(connSetting, S3DatabaseInfo.class);
        return s3DatabaseInfo;
    }

    @Override
    public S3BucketInfo initTableConfig(TableConf conf) {
        String bucketName = conf.getTableName();
        if (StringUtils.isBlank(bucketName)) {
            log.error("S3 init bucket config fail, bucketName is null! dbId: [{}]", conf.getId());
            throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_DESC_EXCEPTION, "数据表配置必填参数缺失，请校验参数配置是否正确!");
        }
        S3BucketInfo tableInfo = new S3BucketInfo();
        tableInfo.setBucketName(bucketName);
        return tableInfo;
    }

    @Override
    public AmazonS3 initDbConnect(S3DatabaseInfo databaseConf, S3BucketInfo tableConf) {
        BasicAWSCredentials credentials = new BasicAWSCredentials(databaseConf.getAccessKey(), databaseConf.getSecretKey());
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride(databaseConf.getSignerOverride());// 凭证验证方式
        clientConfiguration.setProtocol(Protocol.HTTP);// 访问协议
        return AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withClientConfiguration(clientConfiguration)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(databaseConf.getEndpoint(), databaseConf.getRegion()))
                .withPathStyleAccessEnabled(true) // 是否使用路径方式，是的话上。xx.sn/bucketname
                .build();
    }

    @Override
    public void destroyDbConnect(String cacheKey, AmazonS3 connect) throws Exception {
    }
}
