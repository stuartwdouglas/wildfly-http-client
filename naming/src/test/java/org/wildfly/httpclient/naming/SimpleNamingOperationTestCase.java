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

import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NameNotFoundException;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.wildfly.httpclient.common.HTTPTestServer;
import io.undertow.server.handlers.CookieImpl;

/**
 * @author Stuart Douglas
 */
@RunWith(HTTPTestServer.class)
public class SimpleNamingOperationTestCase {

    private static final Map<String, Object> bindings = new ConcurrentHashMap<>();


    @Before
    public void setup() {
        bindings.put("test", "test value");
        bindings.put("comp/UserTransaction", "transaction");
        HTTPTestServer.registerServicesHandler("common/v1/affinity", exchange -> exchange.getResponseCookies().put("JSESSIONID", new CookieImpl("JSESSIONID", "foo")));
        HTTPTestServer.registerServicesHandler("naming", new HttpRemoteNamingService(new Context() {

            @Override
            public Object lookup(Name name) throws NamingException {
                return lookup(name.toString());
            }

            @Override
            public Object lookup(String name) throws NamingException {
                Object res = bindings.get(name);
                if (res == null) {
                    throw new NameNotFoundException();
                }
                return res;
            }

            @Override
            public void bind(Name name, Object obj) throws NamingException {
                bind(name.toString(), obj);
            }

            @Override
            public void bind(String name, Object obj) throws NamingException {
                bindings.put(name, obj);
            }

            @Override
            public void rebind(Name name, Object obj) throws NamingException {
                rebind(name.toString(), obj);
            }

            @Override
            public void rebind(String name, Object obj) throws NamingException {
                bindings.put(name, obj);
            }

            @Override
            public void unbind(Name name) throws NamingException {
                unbind(name.toString());
            }

            @Override
            public void unbind(String name) throws NamingException {
                bindings.remove(name);
            }

            @Override
            public void rename(Name oldName, Name newName) throws NamingException {

            }

            @Override
            public void rename(String oldName, String newName) throws NamingException {
                Object obj = bindings.remove(oldName);
                if (obj == null) {
                    throw new NameNotFoundException();
                }
                bindings.put(newName, obj);
            }

            @Override
            public NamingEnumeration<NameClassPair> list(Name name) throws NamingException {
                return list(name.toString());
            }

            @Override
            public NamingEnumeration<NameClassPair> list(String name) throws NamingException {
                return null;
            }

            @Override
            public NamingEnumeration<Binding> listBindings(Name name) throws NamingException {
                return listBindings(name.toString());
            }

            @Override
            public NamingEnumeration<Binding> listBindings(String name) throws NamingException {
                return null;
            }

            @Override
            public void destroySubcontext(Name name) throws NamingException {
                destroySubcontext(name.toString());
            }

            @Override
            public void destroySubcontext(String name) throws NamingException {

            }

            @Override
            public Context createSubcontext(Name name) throws NamingException {
                return createSubcontext(name.toString());
            }

            @Override
            public Context createSubcontext(String name) throws NamingException {
                return null;
            }

            @Override
            public Object lookupLink(Name name) throws NamingException {
                return lookupLink(name.toString());
            }

            @Override
            public Object lookupLink(String name) throws NamingException {
                return null;
            }

            @Override
            public NameParser getNameParser(Name name) throws NamingException {
                return null;
            }

            @Override
            public NameParser getNameParser(String name) throws NamingException {
                return null;
            }

            @Override
            public Name composeName(Name name, Name prefix) throws NamingException {
                return null;
            }

            @Override
            public String composeName(String name, String prefix) throws NamingException {
                return null;
            }

            @Override
            public Object addToEnvironment(String propName, Object propVal) throws NamingException {
                return null;
            }

            @Override
            public Object removeFromEnvironment(String propName) throws NamingException {
                return null;
            }

            @Override
            public Hashtable<?, ?> getEnvironment() throws NamingException {
                return null;
            }

            @Override
            public void close() throws NamingException {

            }

            @Override
            public String getNameInNamespace() throws NamingException {
                return null;
            }
        }).createHandler());


    }


    @Test
    public void testJNDIlookup() throws NamingException {
        InitialContext ic = createContext();
        Object result = ic.lookup("test");
        Assert.assertEquals("test value", result);
        result = ic.lookup("comp/UserTransaction");
        Assert.assertEquals("transaction", result);
        try {
            ic.lookup("missing");
            Assert.fail();
        } catch (NameNotFoundException expected) {
        }
    }

    @Test
    public void testJNDIBindings() throws NamingException {
        InitialContext ic = createContext();
        try {
            ic.lookup("bound");
            Assert.fail();
        } catch (NameNotFoundException e) {
        }
        ic.bind("bound", "test binding");
        Assert.assertEquals("test binding", ic.lookup("bound"));
        ic.rebind("bound", "test binding 2");
        Assert.assertEquals("test binding 2", ic.lookup("bound"));

//        ic.rename("bound", "bound2");
//        try {
//            ic.lookup("bound");
//            Assert.fail();
//        } catch (NameNotFoundException e) {}
//        Assert.assertEquals("test binding 2", ic.lookup("bound2"));

    }

    private InitialContext createContext() throws NamingException {
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "org.wildfly.naming.client.WildFlyInitialContextFactory");
        env.put(Context.PROVIDER_URL, HTTPTestServer.getDefaultServerURL() + ";http://127.0.0.1:1");
        return new InitialContext(env);
    }

}
