package com.meiya.whalex.db.module.ani;

import com.meiya.whalex.db.entity.ani.BaseDmDatabaseInfo;
import com.meiya.whalex.db.entity.ani.BaseDmKeywordHandler;
import com.meiya.whalex.db.entity.CreateSequenceBean;
import com.meiya.whalex.db.entity.DropSequenceBean;
import com.meiya.whalex.sql.module.SequenceHandler;
import org.apache.commons.lang3.StringUtils;

public class DmSequenceHandler implements SequenceHandler {

    private BaseDmKeywordHandler baseDmKeywordHandler = new BaseDmKeywordHandler();

    private BaseDmDatabaseInfo baseDmDatabaseInfo;

    public <D extends BaseDmDatabaseInfo> DmSequenceHandler(D database) {
        this.baseDmDatabaseInfo = database;
    }

    private String getSequenceName(String sequenceName) {

        if (StringUtils.isBlank(sequenceName)) {
            throw new RuntimeException("序列名称不能为空");
        }

        String schema = baseDmDatabaseInfo.getSchema();
        boolean ignoreCase = baseDmDatabaseInfo.isIgnoreCase();

        if (!ignoreCase) {
            return "\"" + schema + "\"" + "." + "\"" + sequenceName + "\"";
        } else {
            if (baseDmKeywordHandler.isKeyWord(sequenceName)) {
                sequenceName = baseDmKeywordHandler.handler(sequenceName);
            }
            return schema + "." + sequenceName;
        }
    }

    @Override
    public String parseCreateSequenceBean(CreateSequenceBean createSequenceBean) {

        String sequenceName = getSequenceName(createSequenceBean.getSequenceName());


        StringBuilder sb = new StringBuilder();
        sb.append("create sequence ").append(sequenceName);

        //步长
        Integer incrementBy = createSequenceBean.getIncrementBy();
        if (incrementBy != null) {
            sb.append(" increment by ").append(incrementBy);
        }

        //初始值
        Integer startWith = createSequenceBean.getStartWith();
        if (startWith != null) {
            sb.append(" start with ").append(startWith);
        }

        //最大值
        Integer maxValue = createSequenceBean.getMaxValue();
        boolean noMaxValue = createSequenceBean.isNoMaxValue();
        if (noMaxValue) {
            sb.append(" nomaxvalue");
        } else {
            if (maxValue != null) {
                sb.append(" maxvalue ").append(maxValue);
            }
        }

        //最小值
        Integer minValue = createSequenceBean.getMinValue();
        boolean noMinValue = createSequenceBean.isNoMinValue();
        if (noMinValue) {
            sb.append(" nominvalue");
        } else {
            if (maxValue != null) {
                sb.append(" minvalue ").append(minValue);
            }
        }

        //循环
        Boolean cycle = createSequenceBean.getCycle();
        if (cycle != null) {
            if (cycle) {
                sb.append(" cycle");
            } else {
                sb.append(" nocycle");
            }
        }

        //缓存
        Integer cache = createSequenceBean.getCache();
        boolean noCache = createSequenceBean.isNoCache();
        if (noCache) {
            sb.append(" nocache");
        } else {
            if (cache != null) {
                sb.append(" cache ").append(cache);
            }
        }

        return sb.toString();
    }

    @Override
    public String parseDropSequenceBean(DropSequenceBean dropSequenceBean) {
        String sequenceName = getSequenceName(dropSequenceBean.getSequenceName());
        return "drop sequence " + sequenceName;
    }
}
