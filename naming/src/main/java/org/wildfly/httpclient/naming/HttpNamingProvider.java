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

package org.wildfly.httpclient.naming;

import java.net.URI;
import java.security.Principal;

import javax.naming.NamingException;

import org.wildfly.naming.client.NamingProvider;
import org.wildfly.naming.client.ProviderEnvironment;
import org.wildfly.security.auth.client.AuthenticationConfiguration;
import org.wildfly.security.auth.client.PeerIdentity;
import org.wildfly.security.auth.client.PeerIdentityContext;

/**
 * @author Stuart Douglas
 */
public class HttpNamingProvider implements NamingProvider {

    public static final Principal FAKE_PRINCIPAL = new Principal() {
        @Override
        public String getName() {
            return "";
        }
    };
    private final ProviderEnvironment providerEnvironment;

    HttpNamingProvider(final ProviderEnvironment providerEnvironment) {
        this.providerEnvironment = providerEnvironment;
    }

    public ProviderEnvironment getProviderEnvironment() {
        return providerEnvironment;
    }

    public PeerIdentity getPeerIdentityForNaming(final URI location) throws NamingException {
        return new HttpPeerIdentityContext(location).authenticate(null);
    }

    static class HttpPeerIdentity extends PeerIdentity {

        private final URI uri;

        /**
         * Construct a new instance.
         *
         * @param configuration the opaque configuration (must not be {@code null})
         * @param uri           The URI
         */
        protected HttpPeerIdentity(Configuration configuration, URI uri) {
            super(configuration, FAKE_PRINCIPAL);
            this.uri = uri;
        }

        public URI getUri() {
            return uri;
        }
    }

    static class HttpPeerIdentityContext extends PeerIdentityContext {

        private final URI uri;

        HttpPeerIdentityContext(URI uri) {
            this.uri = uri;
        }

        @Override
        public HttpPeerIdentity authenticate(AuthenticationConfiguration authenticationConfiguration) {
            return constructIdentity(configuration -> new HttpPeerIdentity(configuration, uri));
        }
    }
}
