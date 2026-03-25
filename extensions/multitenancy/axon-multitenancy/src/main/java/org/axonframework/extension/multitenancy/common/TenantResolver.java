/*
 * Copyright (c) 2010-2026. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.axonframework.extension.multitenancy.common;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Collection;
import java.util.Collections;
import java.util.function.BiFunction;

/**
 * Resolves the target tenant of a given {@link Message} implementation of type {@code M}.
 *
 * @param <M> the {@link Message} implementation this resolver acts on
 * @author Stefan Dragisic
 * @since 4.6.0
 */
public interface TenantResolver<M extends Message>
        extends BiFunction<M, Collection<TenantDescriptor>, TenantDescriptor> {

    /**
     * Returns {@link TenantDescriptor} for the given {@code message}.
     *
     * @param message the {@link Message} implementation to resolve the target tenant for
     * @param tenants the collection of tenants to resolve the target tenant from. May be empty
     * @return the resolved {@link TenantDescriptor} based on the given {@code message}
     */
    default TenantDescriptor resolveTenant(M message, Collection<TenantDescriptor> tenants) {
        return this.apply(message, Collections.unmodifiableCollection(tenants));
    }

    /**
     * Resolves the tenant from the current {@link ProcessingContext} by extracting the message
     * stored on it and passing it to the given {@code resolver}.
     * <p>
     * This is a convenience method for components that operate within an existing processing context
     * (such as the event store or snapshot store) and need to resolve the tenant from the context's
     * message rather than from a directly available message parameter.
     *
     * @param context  the processing context containing the message
     * @param resolver the resolver to use for tenant extraction
     * @param tenants  the collection of known tenants
     * @return the resolved {@link TenantDescriptor}
     * @throws IllegalStateException if no message is found in the processing context
     */
    static TenantDescriptor fromContext(ProcessingContext context,
                                        TenantResolver<Message> resolver,
                                        Collection<TenantDescriptor> tenants) {
        Message message = Message.fromContext(context);
        if (message == null) {
            throw new IllegalStateException(
                    "Cannot resolve tenant: no message found in ProcessingContext"
            );
        }
        return resolver.resolveTenant(message, tenants);
    }
}
