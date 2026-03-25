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

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.jspecify.annotations.Nullable;

/**
 * Default implementation of {@link TenantResolverRegistry} that stores {@link ComponentBuilder} references
 * for a general and per-message-type {@link TenantResolver}.
 * <p>
 * Type-specific resolvers (command, event, query) take precedence over the general resolver.
 * When a type-specific resolver is not configured, the general resolver is returned as a fallback.
 *
 * @author Theo Emanuelsson
 * @since 5.2.0
 * @see TenantResolverRegistry
 */
@Internal
public class DefaultTenantResolverRegistry implements TenantResolverRegistry {

    private @Nullable ComponentBuilder<TenantResolver<Message>> generalBuilder;
    private @Nullable ComponentBuilder<TenantResolver<? super CommandMessage>> commandBuilder;
    private @Nullable ComponentBuilder<TenantResolver<? super EventMessage>> eventBuilder;
    private @Nullable ComponentBuilder<TenantResolver<? super QueryMessage>> queryBuilder;

    @Override
    public TenantResolverRegistry registerResolver(
            ComponentBuilder<TenantResolver<Message>> resolverBuilder
    ) {
        this.generalBuilder = resolverBuilder;
        return this;
    }

    @Override
    public TenantResolverRegistry registerCommandResolver(
            ComponentBuilder<TenantResolver<? super CommandMessage>> resolverBuilder
    ) {
        this.commandBuilder = resolverBuilder;
        return this;
    }

    @Override
    public TenantResolverRegistry registerEventResolver(
            ComponentBuilder<TenantResolver<? super EventMessage>> resolverBuilder
    ) {
        this.eventBuilder = resolverBuilder;
        return this;
    }

    @Override
    public TenantResolverRegistry registerQueryResolver(
            ComponentBuilder<TenantResolver<? super QueryMessage>> resolverBuilder
    ) {
        this.queryBuilder = resolverBuilder;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public @Nullable TenantResolver<Message> commandResolver(Configuration config) {
        if (commandBuilder != null) {
            return (TenantResolver<Message>) (TenantResolver<?>) commandBuilder.build(config);
        }
        if (generalBuilder != null) {
            return generalBuilder.build(config);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public @Nullable TenantResolver<Message> eventResolver(Configuration config) {
        if (eventBuilder != null) {
            return (TenantResolver<Message>) (TenantResolver<?>) eventBuilder.build(config);
        }
        if (generalBuilder != null) {
            return generalBuilder.build(config);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public @Nullable TenantResolver<Message> queryResolver(Configuration config) {
        if (queryBuilder != null) {
            return (TenantResolver<Message>) (TenantResolver<?>) queryBuilder.build(config);
        }
        if (generalBuilder != null) {
            return generalBuilder.build(config);
        }
        return null;
    }

    @Override
    public @Nullable TenantResolver<Message> resolver(Configuration config) {
        if (generalBuilder != null) {
            return generalBuilder.build(config);
        }
        return null;
    }

    @Override
    public boolean hasResolver() {
        return generalBuilder != null
                || commandBuilder != null
                || eventBuilder != null
                || queryBuilder != null;
    }
}
