package org.apache.tinkerpop.gremlin.driver.ser;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author 黄河森
 * @date 2023/4/23
 * @package org.apache.tinkerpop.gremlin.driver.ser
 * @project whalex-data-driver
 */
public abstract class AbstractGryoMessageSerializerV3d0 extends AbstractMessageSerializer<GryoMapper> {
    private GryoMapper gryoMapper;
    private ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            return AbstractGryoMessageSerializerV3d0.this.gryoMapper.createMapper();
        }
    };
    private static final Charset UTF8 = Charset.forName("UTF-8");
    public static final String TOKEN_CUSTOM = "custom";
    public static final String TOKEN_SERIALIZE_RESULT_TO_STRING = "serializeResultToString";
    public static final String TOKEN_BUFFER_SIZE = "bufferSize";
    public static final String TOKEN_CLASS_RESOLVER_SUPPLIER = "classResolverSupplier";
    protected boolean serializeToString = false;
    private int bufferSize = 4096;

    public AbstractGryoMessageSerializerV3d0(final GryoMapper kryo) {
        this.gryoMapper = kryo;
    }

    GryoMapper.Builder configureBuilder(final GryoMapper.Builder builder, final Map<String, Object> config, final Map<String, Graph> graphs) {
        return builder;
    }

    @Override
    public GryoMapper getMapper() {
        return this.gryoMapper;
    }

    @Override
    public final void configure(final Map<String, Object> config, final Map<String, Graph> graphs) {
        GryoMapper.Builder builder = GryoMapper.build().version(GryoVersion.V3_0);
        this.addIoRegistries(config, builder);
        this.addClassResolverSupplier(config, builder);
        this.addCustomClasses(config, builder);
        this.serializeToString = Boolean.parseBoolean(config.getOrDefault("serializeResultToString", "false").toString());
        this.bufferSize = Integer.parseInt(config.getOrDefault("bufferSize", "4096").toString());
        this.gryoMapper = this.configureBuilder(builder, config, graphs).create();
    }

    private void addClassResolverSupplier(final Map<String, Object> config, final GryoMapper.Builder builder) {
        String className = (String)config.getOrDefault("classResolverSupplier", (Object)null);
        if (className != null && !className.isEmpty()) {
            try {
                Class clazz = Class.forName(className);

                try {
                    Method instanceMethod = this.tryInstanceMethod(clazz);
                    builder.classResolver((Supplier)instanceMethod.invoke((Object)null));
                } catch (Exception var6) {
                    builder.classResolver((Supplier)clazz.newInstance());
                }
            } catch (Exception var7) {
                throw new IllegalStateException(var7);
            }
        }

    }

    private void addCustomClasses(final Map<String, Object> config, final GryoMapper.Builder builder) {
        List<String> classNameList = this.getListStringFromConfig("custom", config);
        classNameList.stream().forEach((serializerDefinition) -> {
            String className;
            Optional serializerName;
            if (serializerDefinition.contains(";")) {
                String[] split = serializerDefinition.split(";");
                if (split.length != 2) {
                    throw new IllegalStateException(String.format("Invalid format for serializer definition [%s] - expected <class>;<serializer-class>", serializerDefinition));
                }

                className = split[0];
                serializerName = Optional.of(split[1]);
            } else {
                serializerName = Optional.empty();
                className = serializerDefinition;
            }

            try {
                Class clazz = Class.forName(className);
                if (serializerName.isPresent()) {
                    Class serializerClazz = Class.forName((String)serializerName.get());
                    Serializer serializer = (Serializer)serializerClazz.newInstance();
                    builder.addCustom(clazz, (kryo) -> {
                        return serializer;
                    });
                } else {
                    builder.addCustom(new Class[]{clazz});
                }

            } catch (Exception var7) {
                throw new IllegalStateException("Class could not be found", var7);
            }
        });
    }

    @Override
    public ResponseMessage deserializeResponse(final ByteBuf msg) throws SerializationException {
        try {
            Kryo kryo = (Kryo)this.kryoThreadLocal.get();
            byte[] payload = new byte[msg.capacity()];
            msg.readBytes(payload);
            Input input = new Input(payload);
            Throwable var5 = null;

            ResponseMessage var6;
            try {
                var6 = (ResponseMessage)kryo.readObject(input, ResponseMessage.class);
            } catch (Throwable var16) {
                var5 = var16;
                throw var16;
            } finally {
                if (input != null) {
                    if (var5 != null) {
                        try {
                            input.close();
                        } catch (Throwable var15) {
                            var5.addSuppressed(var15);
                        }
                    } else {
                        input.close();
                    }
                }

            }

            return var6;
        } catch (Exception var18) {
            logger.warn(String.format("Response [%s] could not be deserialized by %s.", msg, AbstractGryoMessageSerializerV3d0.class.getName()), var18);
            throw new SerializationException(var18);
        }
    }

    @Override
    public ByteBuf serializeResponseAsBinary(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        ByteBuf encodedMessage = null;

        try {
            Kryo kryo = (Kryo)this.kryoThreadLocal.get();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Throwable var6 = null;

            try {
                Output output = new Output(baos, this.bufferSize);
                ResponseMessage msgToWrite = !this.serializeToString ? responseMessage : ResponseMessage.build(responseMessage.getRequestId()).code(responseMessage.getStatus().getCode()).statusAttributes(responseMessage.getStatus().getAttributes()).responseMetaData(responseMessage.getResult().getMeta()).result(this.serializeResultToString(responseMessage)).statusMessage(responseMessage.getStatus().getMessage()).create();
                kryo.writeObject(output, msgToWrite);
                long size = output.total();
                if (size > 2147483647L) {
                    throw new SerializationException(String.format("Message size of %s exceeds allocatable space", size));
                }

                output.flush();
                encodedMessage = allocator.buffer((int)size);
                encodedMessage.writeBytes(baos.toByteArray());
            } catch (Throwable var19) {
                var6 = var19;
                throw var19;
            } finally {
                if (baos != null) {
                    if (var6 != null) {
                        try {
                            baos.close();
                        } catch (Throwable var18) {
                            var6.addSuppressed(var18);
                        }
                    } else {
                        baos.close();
                    }
                }

            }

            return encodedMessage;
        } catch (Exception var21) {
            if (encodedMessage != null) {
                ReferenceCountUtil.release(encodedMessage);
            }

            logger.warn(String.format("Response [%s] could not be serialized by %s.", responseMessage, AbstractGryoMessageSerializerV3d0.class.getName()), var21);
            throw new SerializationException(var21);
        }
    }

    @Override
    public RequestMessage deserializeRequest(final ByteBuf msg) throws SerializationException {
        try {
            Kryo kryo = (Kryo)this.kryoThreadLocal.get();
            byte[] payload = new byte[msg.readableBytes()];
            msg.readBytes(payload);
            Input input = new Input(payload);
            Throwable var5 = null;

            RequestMessage var6;
            try {
                var6 = (RequestMessage)kryo.readObject(input, RequestMessage.class);
            } catch (Throwable var16) {
                var5 = var16;
                throw var16;
            } finally {
                if (input != null) {
                    if (var5 != null) {
                        try {
                            input.close();
                        } catch (Throwable var15) {
                            var5.addSuppressed(var15);
                        }
                    } else {
                        input.close();
                    }
                }

            }

            return var6;
        } catch (Exception var18) {
            logger.warn(String.format("Request [%s] could not be deserialized by %s.", msg, AbstractGryoMessageSerializerV3d0.class.getName()), var18);
            throw new SerializationException(var18);
        }
    }

    @Override
    public ByteBuf serializeRequestAsBinary(final RequestMessage requestMessage, final ByteBufAllocator allocator) throws SerializationException {
        ByteBuf encodedMessage = null;

        try {
            Kryo kryo = (Kryo)this.kryoThreadLocal.get();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Throwable var6 = null;

            try {
                Output output = new Output(baos, this.bufferSize);
                String mimeType = this.mimeTypesSupported()[0];
                output.writeByte(mimeType.length());
                output.write(mimeType.getBytes(UTF8));
                kryo.writeObject(output, requestMessage);
                long size = output.total();
                if (size > 2147483647L) {
                    throw new SerializationException(String.format("Message size of %s exceeds allocatable space", size));
                }

                output.flush();
                encodedMessage = allocator.buffer((int)size);
                encodedMessage.writeBytes(baos.toByteArray());
            } catch (Throwable var19) {
                var6 = var19;
                throw var19;
            } finally {
                if (baos != null) {
                    if (var6 != null) {
                        try {
                            baos.close();
                        } catch (Throwable var18) {
                            var6.addSuppressed(var18);
                        }
                    } else {
                        baos.close();
                    }
                }

            }

            return encodedMessage;
        } catch (Exception var21) {
            if (encodedMessage != null) {
                ReferenceCountUtil.release(encodedMessage);
            }

            logger.warn(String.format("Request [%s] could not be serialized by %s.", requestMessage, AbstractGryoMessageSerializerV3d0.class.getName()), var21);
            throw new SerializationException(var21);
        }
    }

    private Object serializeResultToString(final ResponseMessage msg) {
        if (msg.getResult() == null) {
            return "null";
        } else if (msg.getResult().getData() == null) {
            return "null";
        } else {
            Object o = msg.getResult().getData();
            return o instanceof Collection ? ((Collection)o).stream().map((d) -> {
                return null == d ? "null" : d.toString();
            }).collect(Collectors.toList()) : o.toString();
        }
    }
}
