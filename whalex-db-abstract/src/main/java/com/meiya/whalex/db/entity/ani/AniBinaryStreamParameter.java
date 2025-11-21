package com.meiya.whalex.db.entity.ani;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.InputStream;

/**
 * 关系型数据库 BinaryStream 参数类型支持
 * com.meiya.whalex.jdbc.DatPreparedStatement#setBinaryStream(int, java.io.InputStream)
 *
 * @author 黄河森
 * @date 2024/4/1
 * @package com.meiya.whalex.db.entity.ani
 * @project whalex-data-driver
 * @description AniBinaryStreamParameter
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AniBinaryStreamParameter {

    private InputStream inputStream;

    private long length;

}
