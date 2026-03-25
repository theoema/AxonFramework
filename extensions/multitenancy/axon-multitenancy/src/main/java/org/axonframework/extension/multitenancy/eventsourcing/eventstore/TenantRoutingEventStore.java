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
package org.axonframework.extension.multitenancy.eventsourcing.eventstore;

import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.extension.multitenancy.common.MultiTenantAwareComponent;
import org.axonframework.extension.multitenancy.common.NoSuchTenantException;
import org.axonframework.extension.multitenancy.common.TenantResolver;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;

/**
 * Tenant aware implementation of the {@link EventStore}.
 * <p>
 * Tenant-specific {@code EventStore} segments are resolved from the {@link EventMessage#metadata() event's metadata}.
 * The {@link #open(StreamingCondition, ProcessingContext)} operation throws an
 * {@link UnsupportedOperationException} as multi-tenant streaming requires combining streams from all tenants,
 * which should be handled at a higher level.
 *
 * @see TenantResolver
 *
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @author Theo Emanuelsson
 * @since 4.6.0
 */
public class TenantRoutingEventStore implements EventStore, MultiTenantAwareComponent {

    /**
     * The order in which the {@link TenantRoutingEventStore} is applied as a decorator to the {@link EventStore}.
     * <p>
     * Uses an order HIGHER than {@code InterceptingEventStore} (which is at {@code Integer.MIN_VALUE + 50})
     * to ensure multi-tenant routing is the outermost layer. Interceptors (correlation data, etc.) are applied
     * per-tenant inside each tenant's event store segment, not on the outer multi-tenant store.
     */
    public static final int DECORATION_ORDER = Integer.MIN_VALUE + 75;

    private final Map<TenantDescriptor, EventStore> tenantSegments = new ConcurrentHashMap<>();
    private final List<BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>>> eventsBatchConsumers =
            new CopyOnWriteArrayList<>();
    private final Map<TenantDescriptor, Registration> subscribeRegistrations = new ConcurrentHashMap<>();

    private final TenantEventSegmentFactory tenantSegmentFactory;
    private final TenantResolver<Message> tenantResolver;

    /**
     * Instantiate a {@link TenantRoutingEventStore} with the given {@code tenantSegmentFactory} and
     * {@code tenantResolver}.
     *
     * @param tenantSegmentFactory the factory to create tenant-specific {@link EventStore} segments
     * @param tenantResolver       the resolver to determine the target tenant from a message
     */
    public TenantRoutingEventStore(TenantEventSegmentFactory tenantSegmentFactory,
                                   TenantResolver<Message> tenantResolver) {
        this.tenantSegmentFactory = Objects.requireNonNull(tenantSegmentFactory,
                                                           "TenantEventSegmentFactory may not be null");
        this.tenantResolver = Objects.requireNonNull(tenantResolver,
                                                     "TenantResolver may not be null");
    }

    @Override
    public CompletableFuture<Void> publish(@Nullable ProcessingContext context,
                                           List<? extends EventMessage> events) {
        if (events.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        // Resolve tenant from the ProcessingContext's message (e.g., the command that triggered event publishing).
        // Fall back to the event's own metadata if no context is available.
        Message resolveFrom = context != null ? Message.fromContext(context) : null;
        if (resolveFrom == null) {
            resolveFrom = events.get(0);
        }
        if (resolveFrom == null) {
            throw new IllegalStateException(
                    "Cannot publish to multi-tenant EventStore: no message found in ProcessingContext "
                    + "and no events to resolve tenant from."
            );
        }
        EventStore tenantSegment = resolveTenant(resolveFrom);
        return tenantSegment.publish(context, events);
    }

    @Override
    public Registration subscribe(
            BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> eventsBatchConsumer) {
        eventsBatchConsumers.add(eventsBatchConsumer);

        tenantSegments.forEach((tenant, segment) -> subscribeRegistrations.computeIfAbsent(
                tenant, t -> segment.subscribe(eventsBatchConsumer)
        ));

        return () -> {
            eventsBatchConsumers.remove(eventsBatchConsumer);
            return subscribeRegistrations.values()
                                         .stream()
                                         .map(Registration::cancel)
                                         .reduce((prev, acc) -> prev && acc)
                                         .orElse(false);
        };
    }

    @Override
    public MessageStream<EventMessage> open(StreamingCondition condition,
                                            @Nullable ProcessingContext context) {
        throw new UnsupportedOperationException(
                "Multi-tenant event streaming is not directly supported. "
                        + "Use individual tenant segments or implement a multi-source stream combiner."
        );
    }

    @Override
    public EventStoreTransaction transaction(ProcessingContext processingContext) {
        TenantDescriptor tenant = TenantResolver.fromContext(processingContext, tenantResolver, tenantSegments.keySet());
        EventStore tenantEventStore = tenantSegments.get(tenant);
        if (tenantEventStore == null) {
            throw NoSuchTenantException.forTenantId(tenant.tenantId());
        }

        return tenantEventStore.transaction(processingContext);
    }

    @Override
    public CompletableFuture<TrackingToken> firstToken(@Nullable ProcessingContext context) {
        throw new UnsupportedOperationException(
                "Multi-tenant token operations are not directly supported. "
                        + "Use individual tenant segments."
        );
    }

    @Override
    public CompletableFuture<TrackingToken> latestToken(@Nullable ProcessingContext context) {
        throw new UnsupportedOperationException(
                "Multi-tenant token operations are not directly supported. "
                        + "Use individual tenant segments."
        );
    }

    @Override
    public CompletableFuture<TrackingToken> tokenAt(Instant at, @Nullable ProcessingContext context) {
        throw new UnsupportedOperationException(
                "Multi-tenant token operations are not directly supported. "
                        + "Use individual tenant segments."
        );
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("tenantSegments", tenantSegments);
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        tenantSegments.computeIfAbsent(tenantDescriptor, tenantSegmentFactory::apply);

        return () -> {
            EventStore delegate = unregisterTenant(tenantDescriptor);
            return delegate != null;
        };
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        tenantSegments.computeIfAbsent(tenantDescriptor, k -> {
            EventStore tenantSegment = tenantSegmentFactory.apply(tenantDescriptor);

            eventsBatchConsumers.forEach(consumer -> subscribeRegistrations.computeIfAbsent(
                    tenantDescriptor, t -> tenantSegment.subscribe(consumer)
            ));

            return tenantSegment;
        });

        return () -> {
            EventStore delegate = unregisterTenant(tenantDescriptor);
            return delegate != null;
        };
    }

    private EventStore unregisterTenant(TenantDescriptor tenantDescriptor) {
        Registration remove = subscribeRegistrations.remove(tenantDescriptor);
        if (remove != null) {
            remove.cancel();
        }
        return tenantSegments.remove(tenantDescriptor);
    }

    private EventStore resolveTenant(Message message) {
        TenantDescriptor tenantDescriptor = tenantResolver.resolveTenant(message, tenantSegments.keySet());
        EventStore tenantEventStore = tenantSegments.get(tenantDescriptor);
        if (tenantEventStore == null) {
            throw NoSuchTenantException.forTenantId(tenantDescriptor.tenantId());
        }
        return tenantEventStore;
    }

    /**
     * Returns the tenant segments managed by this {@link TenantRoutingEventStore}.
     *
     * @return the tenant segments managed by this {@link TenantRoutingEventStore}
     */
    public Map<TenantDescriptor, EventStore> tenantSegments() {
        return Collections.unmodifiableMap(tenantSegments);
    }
}
