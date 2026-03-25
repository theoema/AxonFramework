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
package org.axonframework.extension.multitenancy.messaging.commandhandling;

import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.extension.multitenancy.common.MultiTenantAwareComponent;
import org.axonframework.extension.multitenancy.common.NoSuchTenantException;
import org.axonframework.extension.multitenancy.common.TenantResolver;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.CommandHandler;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Implementation of a {@link CommandBus} that is aware of multiple tenant instances of a {@code CommandBus}. Each
 * {@code CommandBus} instance is considered a "tenant".
 * <p>
 * The {@code TenantRoutingCommandBus} relies on a {@link TenantResolver} to dispatch commands via resolved tenant
 * segment of the {@code CommandBus}. {@link TenantCommandSegmentFactory} is a factory to create tenant segments with.
 *
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @author Theo Emanuelsson
 * @since 4.6.0
 */
public class TenantRoutingCommandBus implements CommandBus, MultiTenantAwareComponent {

    /**
     * The order in which the {@link TenantRoutingCommandBus} is applied as a decorator to the {@link CommandBus}.
     */
    public static final int DECORATION_ORDER = Integer.MIN_VALUE + 50;

    private final Map<QualifiedName, CommandHandler> handlers = new ConcurrentHashMap<>();
    private final Map<TenantDescriptor, CommandBus> tenantSegments = new ConcurrentHashMap<>();

    private final TenantCommandSegmentFactory tenantSegmentFactory;
    private final TenantResolver<Message> tenantResolver;

    /**
     * Instantiate a {@link TenantRoutingCommandBus} with the given {@code tenantSegmentFactory} and
     * {@code tenantResolver}.
     *
     * @param tenantSegmentFactory the factory to create tenant-specific {@link CommandBus} segments
     * @param tenantResolver       the resolver to determine the target tenant from a message
     */
    public TenantRoutingCommandBus(TenantCommandSegmentFactory tenantSegmentFactory,
                                   TenantResolver<Message> tenantResolver) {
        this.tenantSegmentFactory = Objects.requireNonNull(tenantSegmentFactory,
                                                           "TenantCommandSegmentFactory may not be null");
        this.tenantResolver = Objects.requireNonNull(tenantResolver,
                                                     "TenantResolver may not be null");
    }

    @Override
    public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                            @Nullable ProcessingContext processingContext) {
        try {
            // Add command to context so downstream components (like EventStore) can resolve tenant
            ProcessingContext contextWithMessage = processingContext != null
                    ? Message.addToContext(processingContext, command)
                    : null;
            return resolveTenant(command).dispatch(command, contextWithMessage);
        } catch (NoSuchTenantException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CommandBus subscribe(QualifiedName name, CommandHandler commandHandler) {
        handlers.computeIfAbsent(name, k -> {
            tenantSegments.forEach((tenant, segment) -> segment.subscribe(name, commandHandler));
            return commandHandler;
        });
        return this;
    }

    /**
     * Returns the tenant segments managed by this {@code TenantRoutingCommandBus}.
     *
     * @return a map of {@link TenantDescriptor} to {@link CommandBus} representing tenant segments
     */
    public Map<TenantDescriptor, CommandBus> tenantSegments() {
        return Collections.unmodifiableMap(tenantSegments);
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        tenantSegments.computeIfAbsent(tenantDescriptor, tenantSegmentFactory::apply);

        return () -> {
            CommandBus delegate = unregisterTenant(tenantDescriptor);
            return delegate != null;
        };
    }

    private CommandBus unregisterTenant(TenantDescriptor tenantDescriptor) {
        return tenantSegments.remove(tenantDescriptor);
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        tenantSegments.computeIfAbsent(tenantDescriptor, tenant -> {
            CommandBus tenantSegment = tenantSegmentFactory.apply(tenantDescriptor);

            handlers.forEach((name, handler) -> tenantSegment.subscribe(name, handler));

            return tenantSegment;
        });

        return () -> {
            CommandBus delegate = unregisterTenant(tenantDescriptor);
            return delegate != null;
        };
    }

    private CommandBus resolveTenant(CommandMessage commandMessage) {
        TenantDescriptor tenantDescriptor = tenantResolver.resolveTenant(commandMessage, tenantSegments.keySet());
        CommandBus tenantCommandBus = tenantSegments.get(tenantDescriptor);
        if (tenantCommandBus == null) {
            throw NoSuchTenantException.forTenantId(tenantDescriptor.tenantId());
        }
        return tenantCommandBus;
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("tenantSegments", tenantSegments);
    }
}
