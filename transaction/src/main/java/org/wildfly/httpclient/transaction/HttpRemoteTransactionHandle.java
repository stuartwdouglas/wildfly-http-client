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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.xa.Xid;

import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Marshalling;
import org.wildfly.httpclient.common.HttpTargetContext;
import org.wildfly.httpclient.common.NoFlushByteOutput;
import org.wildfly.transaction.client.spi.SimpleTransactionControl;
import io.undertow.client.ClientRequest;
import io.undertow.util.Headers;
import io.undertow.util.Methods;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
class HttpRemoteTransactionHandle implements SimpleTransactionControl {

    private final HttpTargetContext targetContext;
    private final AtomicInteger statusRef = new AtomicInteger(Status.STATUS_ACTIVE);
    private final Xid id;

    HttpRemoteTransactionHandle(final Xid id, final HttpTargetContext targetContext) {
        this.id = id;
        this.targetContext = targetContext;
    }

    Xid getId() {
        return id;
    }

    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, SystemException {
        final AtomicInteger statusRef = this.statusRef;
        int oldVal = statusRef.get();
        if (oldVal != Status.STATUS_ACTIVE && oldVal != Status.STATUS_MARKED_ROLLBACK) {
            throw HttpRemoteTransactionMessages.MESSAGES.invalidTxnState();
        }
        synchronized (statusRef) {
            oldVal = statusRef.get();
            if (oldVal == Status.STATUS_MARKED_ROLLBACK) {
                rollback();
                throw HttpRemoteTransactionMessages.MESSAGES.rollbackOnlyRollback();
            }
            if (oldVal != Status.STATUS_ACTIVE) {
                throw HttpRemoteTransactionMessages.MESSAGES.invalidTxnState();
            }
            final CompletableFuture<Void> result = new CompletableFuture<>();
            statusRef.set(Status.STATUS_COMMITTING);
            ClientRequest cr = new ClientRequest()
                    .setMethod(Methods.POST)
                    .setPath(targetContext.getUri().getPath() + TransactionConstants.TXN_V1_UT_COMMIT);
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
            }, (input, response) -> result.complete(null), result::completeExceptionally, null, null);

            try {
                result.get();
                statusRef.set(Status.STATUS_COMMITTED);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                statusRef.set(Status.STATUS_UNKNOWN);
                throw HttpRemoteTransactionMessages.MESSAGES.operationInterrupted();
            } catch (ExecutionException e) {
                try {
                    throw e.getCause();
                } catch (RollbackException ex) {
                    statusRef.set(Status.STATUS_ROLLEDBACK);
                    throw ex;
                } catch (SecurityException ex) {
                    statusRef.set(oldVal);
                    throw ex;
                } catch (HeuristicMixedException | HeuristicRollbackException | SystemException ex) {
                    statusRef.set(Status.STATUS_UNKNOWN);
                    throw ex;
                } catch (Throwable throwable) {
                    SystemException ex = new SystemException(throwable.getMessage());
                    statusRef.set(Status.STATUS_UNKNOWN);
                    ex.initCause(throwable);
                    throw ex;
                }
            }
        }
    }

    public void rollback() throws SecurityException, SystemException {
        final AtomicInteger statusRef = this.statusRef;
        int oldVal = statusRef.get();
        if (oldVal != Status.STATUS_ACTIVE && oldVal != Status.STATUS_MARKED_ROLLBACK) {
            throw HttpRemoteTransactionMessages.MESSAGES.invalidTxnState();
        }
        synchronized (statusRef) {
            oldVal = statusRef.get();
            if (oldVal != Status.STATUS_ACTIVE && oldVal != Status.STATUS_MARKED_ROLLBACK) {
                throw HttpRemoteTransactionMessages.MESSAGES.invalidTxnState();
            }
            statusRef.set(Status.STATUS_ROLLING_BACK);

            final CompletableFuture<Void> result = new CompletableFuture<>();
            statusRef.set(Status.STATUS_COMMITTING);
            ClientRequest cr = new ClientRequest()
                    .setMethod(Methods.POST)
                    .setPath(targetContext.getUri().getPath() + TransactionConstants.TXN_V1_UT_ROLLBACK);
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
            }, (input, response) -> result.complete(null), result::completeExceptionally, null, null);

            try {
                result.get();
                statusRef.set(Status.STATUS_ROLLEDBACK);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                statusRef.set(Status.STATUS_UNKNOWN);
                throw HttpRemoteTransactionMessages.MESSAGES.operationInterrupted();
            } catch (ExecutionException e) {
                try {
                    throw e.getCause();
                } catch (SecurityException ex) {
                    statusRef.set(oldVal);
                    throw ex;
                } catch (SystemException ex) {
                    statusRef.set(Status.STATUS_UNKNOWN);
                    throw ex;
                } catch (Throwable throwable) {
                    SystemException ex = new SystemException(throwable.getMessage());
                    statusRef.set(Status.STATUS_UNKNOWN);
                    ex.initCause(throwable);
                    throw ex;
                }
            }
        }
    }

    public void setRollbackOnly() throws SystemException {
        final AtomicInteger statusRef = this.statusRef;
        int oldVal = statusRef.get();
        if (oldVal == Status.STATUS_MARKED_ROLLBACK) {
            return;
        } else if (oldVal != Status.STATUS_ACTIVE) {
            throw HttpRemoteTransactionMessages.MESSAGES.invalidTxnState();
        }
        synchronized (statusRef) {
            // re-check under lock
            oldVal = statusRef.get();
            if (oldVal == Status.STATUS_MARKED_ROLLBACK) {
                return;
            } else if (oldVal != Status.STATUS_ACTIVE) {
                throw HttpRemoteTransactionMessages.MESSAGES.invalidTxnState();
            }
            statusRef.set(Status.STATUS_MARKED_ROLLBACK);
        }
    }

    @Override
    public <T> T getProviderInterface(Class<T> providerInterfaceType) {
        if(providerInterfaceType == XidProvider.class) {
            return (T) (XidProvider) () -> id;
        }
        return null;
    }
}
