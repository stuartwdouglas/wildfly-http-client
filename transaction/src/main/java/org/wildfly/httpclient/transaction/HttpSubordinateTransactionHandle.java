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

package org.wildfly.httpclient.transaction;

import static org.wildfly.httpclient.transaction.TransactionConstants.TXN_V1_XA_BC;
import static org.wildfly.httpclient.transaction.TransactionConstants.TXN_V1_XA_PREP;
import static org.wildfly.httpclient.transaction.TransactionConstants.TXN_V1_XA_ROLLBACK;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Marshalling;
import org.wildfly.httpclient.common.HttpTargetContext;
import org.wildfly.httpclient.common.NoFlushByteOutput;
import org.wildfly.transaction.client.spi.SubordinateTransactionControl;
import io.undertow.client.ClientRequest;
import io.undertow.client.ClientResponse;
import io.undertow.util.Headers;
import io.undertow.util.Methods;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
class HttpSubordinateTransactionHandle implements SubordinateTransactionControl {

    private final HttpTargetContext targetContext;
    private final Xid id;

    HttpSubordinateTransactionHandle(final Xid id, final HttpTargetContext targetContext) {
        this.id = id;
        this.targetContext = targetContext;
    }

    Xid getId() {
        return id;
    }

    @Override
    public void commit(boolean onePhase) throws XAException {
        String operationPath = TransactionConstants.TXN_V1_XA_COMMIT + (onePhase ? "?opc=true" : "");
        processOperation(operationPath);
    }

    @Override
    public void rollback() throws XAException {
        processOperation(TXN_V1_XA_ROLLBACK);
    }

    @Override
    public void end(int flags) throws XAException {
        //TODO:
    }

    @Override
    public void beforeCompletion() throws XAException {
        processOperation(TXN_V1_XA_BC);
    }

    @Override
    public int prepare() throws XAException {
        boolean readOnly = processOperation(TXN_V1_XA_PREP, (result) -> {
            String header = result.getResponseHeaders().getFirst(TransactionConstants.READ_ONLY);
            return header != null && Boolean.parseBoolean(header);
        });
        return readOnly ? XAResource.XA_RDONLY : XAResource.XA_OK;
    }

    @Override
    public void forget() throws XAException {
        processOperation(TransactionConstants.TXN_V1_XA_FORGET);
    }

    private void processOperation(String operationPath) throws XAException {
        processOperation(operationPath, null);
    }

    private <T> T processOperation(String operationPath, Function<ClientResponse, T> resultFunction) throws XAException {
        final CompletableFuture<T> result = new CompletableFuture<>();
        ClientRequest cr = new ClientRequest()
                .setMethod(Methods.POST)
                .setPath(targetContext.getUri().getPath() + operationPath);
        cr.getRequestHeaders().put(Headers.ACCEPT, TransactionConstants.EXCEPTION);
        cr.getRequestHeaders().put(Headers.CONTENT_TYPE, TransactionConstants.XID_VERSION_1);
        targetContext.sendRequest(cr, output -> {
            Marshaller marshaller = targetContext.createMarshaller(HttpRemoteTransactionPeer.createMarshallingConf());
            marshaller.start(new NoFlushByteOutput(Marshalling.createByteOutput(output)));
            marshaller.writeInt(id.getFormatId());
            final byte[] gtid = id.getGlobalTransactionId();
            marshaller.writeInt(gtid.length);
            marshaller.write(gtid);
            final byte[] bq = id.getBranchQualifier();
            marshaller.writeInt(bq.length);
            marshaller.write(bq);
            marshaller.finish();
            output.close();
        }, (input, response) -> {
            result.complete(resultFunction != null ? resultFunction.apply(response) : null);
        }, result::completeExceptionally, null, null);

        try {
            try {
                return result.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw HttpRemoteTransactionMessages.MESSAGES.interruptedXA(XAException.XAER_RMERR);
            }
        } catch (ExecutionException e) {
            try {
                throw e.getCause();
            } catch (XAException ex) {
                throw ex;
            } catch (Throwable ex) {
                XAException xaException = new XAException(XAException.XAER_RMERR);
                xaException.initCause(ex);
                throw xaException;
            }
        }
    }

}
