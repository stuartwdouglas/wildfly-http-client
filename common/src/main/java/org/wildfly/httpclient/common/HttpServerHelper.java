/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2017 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wildfly.httpclient.common;

import java.io.OutputStream;

import org.jboss.marshalling.ByteOutput;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.MarshallerFactory;
import org.jboss.marshalling.Marshalling;
import org.jboss.marshalling.MarshallingConfiguration;
import org.jboss.marshalling.river.RiverMarshallerFactory;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

/**
 * @author Stuart Douglas
 */
public class HttpServerHelper {

    public static final MarshallerFactory RIVER_MARSHALLER_FACTORY = new RiverMarshallerFactory();

    private HttpServerHelper() {

    }

    public static void sendException(HttpServerExchange exchange, int status, Throwable e) {
        try {
            exchange.setStatusCode(status);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/x-wf-jbmar-exception;version=1");
            final MarshallingConfiguration marshallingConfiguration = new MarshallingConfiguration();
            marshallingConfiguration.setVersion(2);
            final Marshaller marshaller = RIVER_MARSHALLER_FACTORY.createMarshaller(marshallingConfiguration);
            OutputStream outputStream = exchange.getOutputStream();
            final ByteOutput byteOutput = Marshalling.createByteOutput(outputStream);
            // start the marshaller
            marshaller.start(new NoFlushByteOutput(byteOutput));
            marshaller.writeObject(e);
            marshaller.write(0);
            marshaller.finish();
            marshaller.flush();
            exchange.endExchange();
        } catch (Exception ex) {
            HttpClientMessages.MESSAGES.failedToWriteException(ex);
            exchange.endExchange();
        }
    }
}
