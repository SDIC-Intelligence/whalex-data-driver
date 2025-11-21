package com.meiya.whalex.sql.module;

import com.meiya.whalex.db.entity.CreateSequenceBean;
import com.meiya.whalex.db.entity.DropSequenceBean;
import org.apache.commons.lang3.StringUtils;

public interface SequenceHandler {

    String parseCreateSequenceBean(CreateSequenceBean createSequenceBean);

    default String parseDropSequenceBean(DropSequenceBean dropSequenceBean) {
        String sequenceName = dropSequenceBean.getSequenceName();
        if(StringUtils.isBlank(sequenceName)) {
            throw new RuntimeException("序列名称不能为空");
        }
        return "drop sequence " + sequenceName;
    }
}
