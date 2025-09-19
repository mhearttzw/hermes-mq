package com.echelon.hermes.remoting.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

/**
 * 描述：rocketMq的decoder直接继承的LengthFieldBasedFrameDecoder
 *
 * @author jorelwang
 * @create 2025-09-18 20:22
 */
public class CommandDecoder extends ByteToMessageDecoder {

    /**
     * Decode the from one {@link ByteBuf} to an other. This method will be called till either the input
     * {@link ByteBuf} has nothing to read when return from this method or till nothing was read from the input
     * {@link ByteBuf}.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
     * @param in the {@link ByteBuf} from which to read data
     * @param out the {@link List} to which decoded messages should be added
     * @throws Exception is thrown if an error occurs
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // 我们在LengthFieldBasedFrameDecoder中设置了lengthFieldOffset=0, lengthFieldLength=4
        // 所以Netty传入的in ByteBuf已经是去掉了“总长度”字段的、完整的包体。
        // 即 in ByteBuf 的内容为: Header长度(4字节) + Header内容 + Body内容

        // 1、读取header长度
        int headerLength = in.readInt();

        // 2、读取并解码header
        byte[] headerData = new byte[headerLength];
        System.out.printf("headerLength: %d\n", headerLength);
        in.readBytes(headerData);
        RemotingCommand cmd = RemotingCommand.decodeHeader(headerData);

        // 3、读取body
        int bodyLength = in.readableBytes();
        if (bodyLength > 0) {
            byte[] bodyData = new byte[bodyLength];
            in.readBytes(bodyData);
            cmd.setBody(bodyData);
        }

        out.add(cmd);
    }
}
