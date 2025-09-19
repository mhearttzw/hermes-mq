package com.echelon.hermes.remoting.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * 描述：TODO...
 *
 * @author jorelwang
 * @create 2025-09-18 09:48
 */
public class CommandEncoder extends MessageToByteEncoder<RemotingCommand> {

    /**
     * 协议格式:
     * 总长度(4字节) + Header长度(4字节) + Header内容 + Body内容
     * <p>
     * Encode a message into a {@link ByteBuf}. This method will be called for each written message that can be handled
     * by this encoder.
     *</p>
     * @param ctx the {@link ChannelHandlerContext} which this {@link MessageToByteEncoder} belongs to
     * @param cmd the message to encode
     * @param out the {@link ByteBuf} into which the encoded message will be written
     * @throws Exception is thrown if an error occurs
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand cmd, ByteBuf out) throws Exception {
        // 1、编码Header
        byte[] headerData = cmd.encodeHeader();
        int headerLength = headerData.length;

        // 2、编码body
        byte[] bodyData = cmd.getBody();
        int bodyLength = (bodyData != null) ?  bodyData.length : 0;

        // 3、计算总长度
        int totalLength = 4 + headerLength + bodyLength;

        // 4、写入ByteBuf
        out.writeInt(totalLength);
        out.writeInt(headerLength);
        out.writeBytes(headerData);
        if (bodyData != null) {
            out.writeBytes(bodyData);
        }

    }
}
