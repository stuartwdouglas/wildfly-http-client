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

package org.wildfly.httpclient.ejb;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import javax.ejb.EJBHome;
import javax.ejb.NoSuchEJBException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;

import org.jboss.ejb.client.Affinity;
import org.jboss.ejb.client.EJBHomeLocator;
import org.jboss.ejb.client.EJBIdentifier;
import org.jboss.ejb.client.EJBLocator;
import org.jboss.ejb.client.EJBMethodLocator;
import org.jboss.ejb.client.SessionID;
import org.jboss.ejb.client.StatefulEJBLocator;
import org.jboss.ejb.client.StatelessEJBLocator;
import org.jboss.ejb.server.Association;
import org.jboss.ejb.server.CancelHandle;
import org.jboss.ejb.server.InvocationRequest;
import org.jboss.marshalling.ByteOutput;
import org.jboss.marshalling.InputStreamByteInput;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Marshalling;
import org.jboss.marshalling.MarshallingConfiguration;
import org.jboss.marshalling.Unmarshaller;
import org.wildfly.common.annotation.NotNull;
import org.wildfly.httpclient.common.ContentType;
import org.wildfly.httpclient.common.ElytronIdentityHandler;
import org.wildfly.httpclient.common.HttpServerHelper;
import org.wildfly.httpclient.common.NoFlushByteOutput;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.transaction.client.ImportResult;
import org.wildfly.transaction.client.LocalTransaction;
import org.wildfly.transaction.client.LocalTransactionContext;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.Cookie;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;

/**
 * @author Stuart Douglas
 */
class HttpInvocationHandler extends RemoteHTTPHandler {

    private final Association association;
    private final ExecutorService executorService;
    private final LocalTransactionContext localTransactionContext;
    private final Map<InvocationIdentifier, CancelHandle> cancellationFlags;

    HttpInvocationHandler(Association association, ExecutorService executorService, LocalTransactionContext localTransactionContext, Map<InvocationIdentifier, CancelHandle> cancellationFlags) {
        super(executorService);
        this.association = association;
        this.executorService = executorService;
        this.localTransactionContext = localTransactionContext;
        this.cancellationFlags = cancellationFlags;
    }

    @Override
    protected void handleInternal(HttpServerExchange exchange) throws Exception {
        String ct = exchange.getRequestHeaders().getFirst(Headers.CONTENT_TYPE);
        ContentType contentType = ContentType.parse(ct);
        if (contentType == null || contentType.getVersion() != 1 || !EjbHeaders.INVOCATION.equals(contentType.getType())) {
            exchange.setStatusCode(StatusCodes.BAD_REQUEST);
            EjbHttpClientMessages.MESSAGES.debugf("Bad content type %s", ct);
            return;
        }

        String relativePath = exchange.getRelativePath();
        if(relativePath.startsWith("/")) {
            relativePath = relativePath.substring(1);
        }
        String[] parts = relativePath.split("/");
        if(parts.length < 7) {
            exchange.setStatusCode(StatusCodes.NOT_FOUND);
            return;
        }
        final String app = handleDash(parts[0]);
        final String module = handleDash(parts[1]);
        final String distinct = handleDash(parts[2]);
        final String bean = parts[3];


        String originalSessionId = handleDash(parts[4]);
        final byte[] sessionID = originalSessionId.isEmpty() ? null : Base64.getUrlDecoder().decode(originalSessionId);
        String viewName = parts[5];
        String method = parts[6];
        String[] parameterTypeNames = new String[parts.length - 7];
        System.arraycopy(parts, 7, parameterTypeNames, 0, parameterTypeNames.length);
        Cookie cookie = exchange.getRequestCookies().get(EjbHttpService.JSESSIONID);
        final String sessionAffinity = cookie != null ? cookie.getValue() : null;
        final EJBIdentifier ejbIdentifier = new EJBIdentifier(app, module, bean, distinct);

        final String cancellationId = exchange.getRequestHeaders().getFirst(EjbHeaders.INVOCATION_ID);
        final InvocationIdentifier identifier;
        if(cancellationId != null && sessionAffinity != null) {
            identifier = new InvocationIdentifier(cancellationId, sessionAffinity);
        } else {
            identifier = null;
        }

        exchange.dispatch(executorService, () -> {
            CancelHandle handle = association.receiveInvocationRequest(new InvocationRequest() {

                @Override
                public SocketAddress getPeerAddress() {
                    return exchange.getSourceAddress();
                }

                @Override
                public SocketAddress getLocalAddress() {
                    return exchange.getDestinationAddress();
                }

                @Override
                public Resolved getRequestContent(ClassLoader classLoader) throws IOException, ClassNotFoundException {

                    Object[] methodParams = new Object[parameterTypeNames.length];
                    final Class<?> view = Class.forName(viewName, false, classLoader);
                    final MarshallingConfiguration marshallingConfiguration = new MarshallingConfiguration();
                    marshallingConfiguration.setClassTable(ProtocolV1ClassTable.INSTANCE);
                    marshallingConfiguration.setObjectTable(ProtocolV1ObjectTable.INSTANCE);
                    marshallingConfiguration.setVersion(2);
                    Unmarshaller unmarshaller = HttpServerHelper.RIVER_MARSHALLER_FACTORY.createUnmarshaller(marshallingConfiguration);

                    try (InputStream inputStream = exchange.getInputStream()) {
                        unmarshaller.start(new InputStreamByteInput(inputStream));
                        ReceivedTransaction txConfig = readTransaction(unmarshaller);


                        final Transaction transaction;
                        if (txConfig == null || localTransactionContext == null) { //the TX context may be null in unit tests
                            transaction = null;
                        } else {
                            try {
                                ImportResult<LocalTransaction> result = localTransactionContext.findOrImportTransaction(txConfig.getXid(), txConfig.getRemainingTime());
                                transaction = result.getTransaction();
                            } catch (XAException e) {
                                throw new IllegalStateException(e); //TODO: what to do here?
                            }
                        }
                        for (int i = 0; i < parameterTypeNames.length; ++i) {
                            methodParams[i] = unmarshaller.readObject();
                        }
                        final Map<String, Object> privateAttachments;
                        final Map<String, Object> contextData;
                        int attachementCount = PackedInteger.readPackedInteger(unmarshaller);
                        if (attachementCount > 0) {
                            contextData = new HashMap<>();
                            for (int i = 0; i < attachementCount - 1; ++i) {
                                String key = (String) unmarshaller.readObject();
                                Object value = unmarshaller.readObject();
                                contextData.put(key, value);
                            }
                            privateAttachments = (Map<String, Object>) unmarshaller.readObject();
                        } else {
                            contextData = Collections.emptyMap();
                            privateAttachments = Collections.emptyMap();
                        }

                        unmarshaller.finish();

                        EJBLocator<?> locator;
                        if (EJBHome.class.isAssignableFrom(view)) {
                            locator = new EJBHomeLocator(view, app, module, bean, distinct, Affinity.LOCAL); //TODO: what is the correct affinity?
                        } else if (sessionID != null) {
                            locator = new StatefulEJBLocator<>(view, app, module, bean, distinct,
                                    SessionID.createSessionID(sessionID), Affinity.LOCAL);
                        } else {
                            locator = new StatelessEJBLocator<>(view, app, module, bean, distinct, Affinity.LOCAL);
                        }

                        return new ResolvedInvocation(privateAttachments, methodParams, locator, exchange, marshallingConfiguration, sessionAffinity, transaction, identifier);
                    } catch (IOException | ClassNotFoundException e) {
                        throw e;
                    } catch (Throwable e) {
                        throw new IOException(e);
                    }
                }

                @Override
                public EJBMethodLocator getMethodLocator() {
                    return new EJBMethodLocator(method, parameterTypeNames);
                }

                @Override
                public void writeNoSuchMethod() {
                    if(identifier != null) {
                        cancellationFlags.remove(identifier);
                    }
                    HttpServerHelper.sendException(exchange, StatusCodes.NOT_FOUND, EjbHttpClientMessages.MESSAGES.noSuchMethod());
                }

                @Override
                public void writeSessionNotActive() {
                    if(identifier != null) {
                        cancellationFlags.remove(identifier);
                    }
                    HttpServerHelper.sendException(exchange, StatusCodes.INTERNAL_SERVER_ERROR, EjbHttpClientMessages.MESSAGES.sessionNotActive());
                }

                @Override
                public void writeWrongViewType() {
                    if(identifier != null) {
                        cancellationFlags.remove(identifier);
                    }
                    HttpServerHelper.sendException(exchange, StatusCodes.NOT_FOUND, EjbHttpClientMessages.MESSAGES.wrongViewType());
                }

                @Override
                public Executor getRequestExecutor() {
                    return executorService == null ? exchange.getIoThread().getWorker() : executorService;
                }

                @Override
                public String getProtocol() {
                    return exchange.getProtocol().toString();
                }

                @Override
                public boolean isBlockingCaller() {
                    return false;
                }

                @Override
                public EJBIdentifier getEJBIdentifier() {
                    return ejbIdentifier;
                }

//                @Override
                public SecurityIdentity getSecurityIdentity() {
                    return exchange.getAttachment(ElytronIdentityHandler.IDENTITY_KEY);
                }

                @Override
                public void writeException(@NotNull Exception exception) {
                    if(identifier != null) {
                        cancellationFlags.remove(identifier);
                    }
                    HttpServerHelper.sendException(exchange, StatusCodes.INTERNAL_SERVER_ERROR, exception);
                }

                @Override
                public void writeNoSuchEJB() {
                    if(identifier != null) {
                        cancellationFlags.remove(identifier);
                    }
                    HttpServerHelper.sendException(exchange, StatusCodes.NOT_FOUND, new NoSuchEJBException());
                }

                @Override
                public void writeCancelResponse() {
                    if(identifier != null) {
                        cancellationFlags.remove(identifier);
                    }
                    //we don't actually need to implement this method
                }

                @Override
                public void writeNotStateful() {
                    if(identifier != null) {
                        cancellationFlags.remove(identifier);
                    }
                    HttpServerHelper.sendException(exchange, StatusCodes.INTERNAL_SERVER_ERROR, EjbHttpClientMessages.MESSAGES.notStateful());
                }

                @Override
                public void convertToStateful(@NotNull SessionID sessionId) throws IllegalArgumentException, IllegalStateException {
                    throw new RuntimeException("nyi");
                }
            });
            if(handle != null && identifier != null) {
                cancellationFlags.put(identifier, handle);
            }
        });
    }

    private static String handleDash(String s) {
        if (s.equals("-")) {
            return "";
        }
        return s;
    }

    class ResolvedInvocation implements InvocationRequest.Resolved {
        private final Map<String, Object> privateAttachments;
        private final Object[] methodParams;
        private final EJBLocator<?> locator;
        private final HttpServerExchange exchange;
        private final MarshallingConfiguration marshallingConfiguration;
        private final String sessionAffinity;
        private final Transaction transaction;
        private final InvocationIdentifier identifier;

        public ResolvedInvocation(Map<String, Object> privateAttachments, Object[] methodParams, EJBLocator<?> locator, HttpServerExchange exchange, MarshallingConfiguration marshallingConfiguration, String sessionAffinity, Transaction transaction, final InvocationIdentifier identifier) {
            this.privateAttachments = privateAttachments;
            this.methodParams = methodParams;
            this.locator = locator;
            this.exchange = exchange;
            this.marshallingConfiguration = marshallingConfiguration;
            this.sessionAffinity = sessionAffinity;
            this.transaction = transaction;
            this.identifier = identifier;
        }

        @Override
        public Map<String, Object> getAttachments() {
            return privateAttachments;
        }

        @Override
        public Object[] getParameters() {
            return methodParams;
        }

        @Override
        public EJBLocator<?> getEJBLocator() {
            return locator;
        }

        @Override
        public boolean hasTransaction() {
            return transaction != null;
        }

        @Override
        public Transaction getTransaction() throws SystemException, IllegalStateException {
            return transaction;
        }

        String getSessionAffinity() {
            return sessionAffinity;
        }

        HttpServerExchange getExchange() {
            return exchange;
        }

        @Override
        public void writeInvocationResult(Object result) {
            if(identifier != null) {
                cancellationFlags.remove(identifier);
            }
            try {
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, EjbHeaders.EJB_RESPONSE_VERSION_ONE.toString());
//                                    if (output.getSessionAffinity() != null) {
//                                        exchange.getResponseCookies().put("JSESSIONID", new CookieImpl("JSESSIONID", output.getSessionAffinity()).setPath(WILDFLY_SERVICES));
//                                    }
                final Marshaller marshaller = HttpServerHelper.RIVER_MARSHALLER_FACTORY.createMarshaller(marshallingConfiguration);
                OutputStream outputStream = exchange.getOutputStream();
                final ByteOutput byteOutput = Marshalling.createByteOutput(outputStream);
                // start the marshaller
                marshaller.start(new NoFlushByteOutput(byteOutput));
                marshaller.writeObject(result);
                marshaller.write(0);
                marshaller.finish();
                marshaller.flush();
                exchange.endExchange();
            } catch (Exception e) {
                HttpServerHelper.sendException(exchange, 500, e);
            }
        }
    }
}
