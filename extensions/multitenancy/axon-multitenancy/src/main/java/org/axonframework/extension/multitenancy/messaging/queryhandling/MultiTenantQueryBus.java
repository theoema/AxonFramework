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
package org.axonframework.extension.multitenancy.messaging.queryhandling;

import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.extension.multitenancy.common.MultiTenantAwareComponent;
import org.axonframework.extension.multitenancy.common.NoSuchTenantException;
import org.axonframework.extension.multitenancy.common.TargetTenantResolver;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryHandler;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Implementation of a {@link QueryBus} that is aware of multiple tenant instances of a {@code QueryBus}. Each
 * {@code QueryBus} instance is considered a "tenant".
 * <p>
 * The {@code MultiTenantQueryBus} relies on a {@link TargetTenantResolver} to dispatch queries via resolved tenant
 * segment of the {@code QueryBus}. {@link TenantQuerySegmentFactory} is a factory to create the tenant segment.
 *
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @author Theo Emanuelsson
 * @since 4.6.0
 */
public class MultiTenantQueryBus implements QueryBus, MultiTenantAwareComponent {

    /**
     * The order in which the {@link MultiTenantQueryBus} is applied as a decorator to the {@link QueryBus}.
     */
    public static final int DECORATION_ORDER = Integer.MIN_VALUE + 50;

    private final Map<QualifiedName, QueryHandler> handlers = new ConcurrentHashMap<>();
    private final Map<TenantDescriptor, QueryBus> tenantSegments = new ConcurrentHashMap<>();

    private final TenantQuerySegmentFactory tenantSegmentFactory;
    private final TargetTenantResolver<Message> targetTenantResolver;

    /**
     * Instantiate a {@link MultiTenantQueryBus} with the given {@code tenantSegmentFactory} and
     * {@code targetTenantResolver}.
     *
     * @param tenantSegmentFactory  the factory to create tenant-specific {@link QueryBus} segments
     * @param targetTenantResolver  the resolver to determine the target tenant from a {@link Message}
     */
    public MultiTenantQueryBus(TenantQuerySegmentFactory tenantSegmentFactory,
                               TargetTenantResolver<Message> targetTenantResolver) {
        this.tenantSegmentFactory = Objects.requireNonNull(tenantSegmentFactory,
                                                           "TenantQuerySegmentFactory may not be null");
        this.targetTenantResolver = Objects.requireNonNull(targetTenantResolver,
                                                           "TargetTenantResolver may not be null");
    }

    @Override
    public MessageStream<QueryResponseMessage> query(QueryMessage query,
                                                     @Nullable ProcessingContext context) {
        try {
            return resolveTenant(query).query(query, context);
        } catch (NoSuchTenantException e) {
            return MessageStream.failed(e);
        }
    }

    @Override
    public MessageStream<QueryResponseMessage> subscriptionQuery(QueryMessage query,
                                                                  @Nullable ProcessingContext context,
                                                                  int updateBufferSize) {
        try {
            return resolveTenant(query).subscriptionQuery(query, context, updateBufferSize);
        } catch (NoSuchTenantException e) {
            return MessageStream.failed(e);
        }
    }

    @Override
    public MessageStream<SubscriptionQueryUpdateMessage> subscribeToUpdates(QueryMessage query,
                                                                             int updateBufferSize) {
        try {
            return resolveTenant(query).subscribeToUpdates(query, updateBufferSize);
        } catch (NoSuchTenantException e) {
            return MessageStream.failed(e);
        }
    }

    @Override
    public CompletableFuture<Void> emitUpdate(Predicate<QueryMessage> filter,
                                              Supplier<SubscriptionQueryUpdateMessage> updateSupplier,
                                              @Nullable ProcessingContext context) {
        try {
            return resolveTenantFromContext(context).emitUpdate(filter, updateSupplier, context);
        } catch (NoSuchTenantException | IllegalStateException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> completeSubscriptions(Predicate<QueryMessage> filter,
                                                         @Nullable ProcessingContext context) {
        try {
            return resolveTenantFromContext(context).completeSubscriptions(filter, context);
        } catch (NoSuchTenantException | IllegalStateException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> completeSubscriptionsExceptionally(Predicate<QueryMessage> filter,
                                                                       Throwable cause,
                                                                       @Nullable ProcessingContext context) {
        try {
            return resolveTenantFromContext(context).completeSubscriptionsExceptionally(filter, cause, context);
        } catch (NoSuchTenantException | IllegalStateException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private QueryBus resolveTenantFromContext(@Nullable ProcessingContext context) {
        if (context == null) {
            throw new IllegalStateException(
                    "Cannot resolve tenant for subscription query update: ProcessingContext is required"
            );
        }
        Message message = Message.fromContext(context);
        if (message == null) {
            throw new IllegalStateException(
                    "Cannot resolve tenant for subscription query update: no message found in ProcessingContext"
            );
        }
        TenantDescriptor tenantDescriptor = targetTenantResolver.resolveTenant(message, tenantSegments.keySet());
        QueryBus tenantQueryBus = tenantSegments.get(tenantDescriptor);
        if (tenantQueryBus == null) {
            throw NoSuchTenantException.forTenantId(tenantDescriptor.tenantId());
        }
        return tenantQueryBus;
    }

    @Override
    public QueryBus subscribe(QualifiedName queryName, QueryHandler queryHandler) {
        handlers.computeIfAbsent(queryName, k -> {
            tenantSegments.forEach((tenant, segment) -> segment.subscribe(queryName, queryHandler));
            return queryHandler;
        });
        return this;
    }

    /**
     * Returns the tenant segments managed by this {@code MultiTenantQueryBus}.
     *
     * @return a map of {@link TenantDescriptor} to {@link QueryBus} representing tenant segments
     */
    public Map<TenantDescriptor, QueryBus> tenantSegments() {
        return tenantSegments;
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        QueryBus tenantSegment = tenantSegmentFactory.apply(tenantDescriptor);
        tenantSegments.putIfAbsent(tenantDescriptor, tenantSegment);

        return () -> {
            QueryBus delegate = unregisterTenant(tenantDescriptor);
            return delegate != null;
        };
    }

    private QueryBus unregisterTenant(TenantDescriptor tenantDescriptor) {
        return tenantSegments.remove(tenantDescriptor);
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        tenantSegments.computeIfAbsent(tenantDescriptor, tenant -> {
            QueryBus tenantSegment = tenantSegmentFactory.apply(tenantDescriptor);

            handlers.forEach((queryName, queryHandler) -> tenantSegment.subscribe(queryName, queryHandler));

            return tenantSegment;
        });

        return () -> {
            QueryBus delegate = unregisterTenant(tenantDescriptor);
            return delegate != null;
        };
    }

    private QueryBus resolveTenant(QueryMessage queryMessage) {
        TenantDescriptor tenantDescriptor = targetTenantResolver.resolveTenant(queryMessage, tenantSegments.keySet());
        QueryBus tenantQueryBus = tenantSegments.get(tenantDescriptor);
        if (tenantQueryBus == null) {
            throw NoSuchTenantException.forTenantId(tenantDescriptor.tenantId());
        }
        return tenantQueryBus;
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("tenantSegments", tenantSegments);
    }
}
