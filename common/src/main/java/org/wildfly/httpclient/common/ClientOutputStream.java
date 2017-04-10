/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2014 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.wildfly.httpclient.common;

import io.undertow.UndertowMessages;
import io.undertow.client.ClientExchange;
import io.undertow.connector.ByteBufferPool;
import io.undertow.connector.PooledByteBuffer;
import org.xnio.channels.Channels;
import org.xnio.channels.StreamSinkChannel;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.xnio.Bits.anyAreSet;

/**
 * Buffering output stream that wraps a channel.
 * <p>
 * This stream delays channel creation, so if a response will fit in the buffer it is not necessary to
 * set the content length header.
 *
 * @author Stuart Douglas
 */
public class ClientOutputStream extends OutputStream {

    private final ClientExchange exchange;
    private ByteBuffer buffer;
    private PooledByteBuffer pooledBuffer;
    private StreamSinkChannel channel;
    private int state;

    private static final int FLAG_CLOSED = 1;
    private static final int FLAG_WRITE_STARTED = 1 << 1;

    private static final int MAX_BUFFERS_TO_ALLOCATE = 10;

    private final long start;

    /**
     * Construct a new instance.  No write timeout is configured.
     *
     * @param exchange The exchange
     * @param start
     */
    public ClientOutputStream(ClientExchange exchange, long start) {
        this.exchange = exchange;
        this.start = start;
    }


    /**
     * {@inheritDoc}
     */
    public void write(final int b) throws IOException {
        write(new byte[]{(byte) b}, 0, 1);
    }

    /**
     * {@inheritDoc}
     */
    public void write(final byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    /**
     * {@inheritDoc}
     */
    public void write(final byte[] b, final int off, final int len) throws IOException {
        if (len < 1) {
            return;
        }
        if (Thread.currentThread() == exchange.getConnection().getIoThread()) {
            throw UndertowMessages.MESSAGES.blockingIoFromIOThread();
        }
        if (anyAreSet(state, FLAG_CLOSED)) {
            throw UndertowMessages.MESSAGES.streamIsClosed();
        }
        ByteBuffer buffer = buffer();
        if (buffer.remaining() < len) {
            if (buffer.remaining() < len) {

                //so what we have will not fit.
                //We allocate multiple buffers up to MAX_BUFFERS_TO_ALLOCATE
                //and put it in them
                //if it still dopes not fit we loop, re-using these buffers

                StreamSinkChannel channel = this.channel;
                if (channel == null) {
                    this.channel = channel = exchange.getRequestChannel();
                }
                final ByteBufferPool bufferPool = exchange.getConnection().getBufferPool();
                ByteBuffer[] buffers = new ByteBuffer[MAX_BUFFERS_TO_ALLOCATE + 1];
                PooledByteBuffer[] pooledBuffers = new PooledByteBuffer[MAX_BUFFERS_TO_ALLOCATE];
                try {
                    buffers[0] = buffer;
                    int bytesWritten = 0;
                    int rem = buffer.remaining();
                    buffer.put(b, bytesWritten + off, rem);
                    buffer.flip();
                    bytesWritten += rem;
                    int bufferCount = 1;
                    for (int i = 0; i < MAX_BUFFERS_TO_ALLOCATE; ++i) {
                        PooledByteBuffer pooled = bufferPool.allocate();
                        pooledBuffers[bufferCount - 1] = pooled;
                        buffers[bufferCount++] = pooled.getBuffer();
                        ByteBuffer cb = pooled.getBuffer();
                        int toWrite = len - bytesWritten;
                        if (toWrite > cb.remaining()) {
                            rem = cb.remaining();
                            cb.put(b, bytesWritten + off, rem);
                            cb.flip();
                            bytesWritten += rem;
                        } else {
                            cb.put(b, bytesWritten + off, len - bytesWritten);
                            bytesWritten = len;
                            cb.flip();
                            break;
                        }
                    }
                    Channels.writeBlocking(channel, buffers, 0, bufferCount);
                    while (bytesWritten < len) {
                        //ok, it did not fit, loop and loop and loop until it is done
                        bufferCount = 0;
                        for (int i = 0; i < MAX_BUFFERS_TO_ALLOCATE + 1; ++i) {
                            ByteBuffer cb = buffers[i];
                            cb.clear();
                            bufferCount++;
                            int toWrite = len - bytesWritten;
                            if (toWrite > cb.remaining()) {
                                rem = cb.remaining();
                                cb.put(b, bytesWritten + off, rem);
                                cb.flip();
                                bytesWritten += rem;
                            } else {
                                cb.put(b, bytesWritten + off, len - bytesWritten);
                                bytesWritten = len;
                                cb.flip();
                                break;
                            }
                        }
                        Channels.writeBlocking(channel, buffers, 0, bufferCount);
                    }
                    buffer.clear();
                } finally {
                    for (int i = 0; i < pooledBuffers.length; ++i) {
                        PooledByteBuffer p = pooledBuffers[i];
                        if (p == null) {
                            break;
                        }
                        p.close();
                    }
                }
            } else {
                buffer.put(b, off, len);
                if (buffer.remaining() == 0) {
                    writeBufferBlocking(false);
                }
            }
        } else {
            buffer.put(b, off, len);
            if (buffer.remaining() == 0) {
                writeBufferBlocking(false);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public void flush() throws IOException {
        //ignore
    }

    private void writeBufferBlocking(final boolean writeFinal) throws IOException {
        if (channel == null) {
            channel = exchange.getRequestChannel();
        }
        buffer.flip();

        while (buffer.hasRemaining()) {
            if (writeFinal) {
                channel.writeFinal(buffer);
            } else {
                channel.write(buffer);
            }
            if (buffer.hasRemaining()) {
                channel.awaitWritable();
            }
        }
        buffer.clear();
        state |= FLAG_WRITE_STARTED;
    }

    /**
     * {@inheritDoc}
     */
    public void close() throws IOException {
        if (anyAreSet(state, FLAG_CLOSED)) return;
        try {
            state |= FLAG_CLOSED;
            if (buffer != null) {
                writeBufferBlocking(true);
            }
            if (channel == null) {
                channel = exchange.getRequestChannel();
            }
            if (channel == null) {
                return;
            }
            StreamSinkChannel channel = this.channel;
            channel.shutdownWrites();
            Channels.flushBlocking(channel);
        } finally {
            if (pooledBuffer != null) {
                pooledBuffer.close();
                buffer = null;
            } else {
                buffer = null;
            }
        }
    }

    private ByteBuffer buffer() {
        ByteBuffer buffer = this.buffer;
        if (buffer != null) {
            return buffer;
        }
        this.pooledBuffer = exchange.getConnection().getBufferPool().allocate();
        this.buffer = pooledBuffer.getBuffer();
        return this.buffer;
    }

}
