package org.apache.tinkerpop.gremlin.driver.ser;

import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;

/**
 * @author 黄河森
 * @date 2023/4/23
 * @package org.apache.tinkerpop.gremlin.driver.ser
 * @project whalex-data-driver
 */
public final class GryoMessageSerializerV3d0 extends AbstractGryoMessageSerializerV3d0 {
    private static final String MIME_TYPE = "application/vnd.gremlin-v3.0+gryo";
    private static final String MIME_TYPE_STRINGD = "application/vnd.gremlin-v3.0+gryo-stringd";

    public GryoMessageSerializerV3d0() {
        super(GryoMapper.build().version(GryoVersion.V3_0).create());
    }

    public GryoMessageSerializerV3d0(final GryoMapper.Builder kryo) {
        super(kryo.version(GryoVersion.V3_0).create());
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{this.serializeToString ? "application/vnd.gremlin-v3.0+gryo-stringd" : "application/vnd.gremlin-v3.0+gryo"};
    }
}
