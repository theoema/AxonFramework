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
package org.axonframework.extension.multitenancy.eventsourcing.eventstore.jpa;

import jakarta.persistence.EntityManagerFactory;
import org.axonframework.extension.multitenancy.eventsourcing.eventstore.TenantEventSegmentFactory;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.eventsourcing.eventstore.AnnotationBasedTagResolver;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.axonframework.eventsourcing.eventstore.jpa.AggregateBasedJpaEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.jpa.AggregateBasedJpaEventStorageEngineConfiguration;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * A {@link TenantEventSegmentFactory} that creates JPA-backed {@link EventStore} instances for each tenant.
 * <p>
 * This factory uses a provider function to obtain tenant-specific {@link EntityManagerFactory} instances.
 * Each tenant gets its own {@link EventStore} backed by {@link AggregateBasedJpaEventStorageEngine},
 * ensuring data isolation between tenants.
 * <p>
 * Event stores are cached per tenant to avoid creating multiple instances for the same tenant.
 * <p>
 * Example usage:
 * <pre>{@code
 * JpaTenantEventSegmentFactory factory = new JpaTenantEventSegmentFactory(
 *     tenant -> getEntityManagerFactoryForTenant(tenant),
 *     eventConverter
 * );
 *
 * // Register it with the component registry
 * configurer.componentRegistry(cr ->
 *     cr.registerComponent(TenantEventSegmentFactory.class, config -> factory)
 * );
 * }</pre>
 *
 * @author Theo Emanuelsson
 * @since 5.2.0
 * @see TenantEventSegmentFactory
 * @see AggregateBasedJpaEventStorageEngine
 */
public class JpaTenantEventSegmentFactory implements TenantEventSegmentFactory {

    private static final Logger logger = LoggerFactory.getLogger(JpaTenantEventSegmentFactory.class);

    private final Function<TenantDescriptor, EntityManagerFactory> emfProvider;
    private final EventConverter eventConverter;
    private final UnaryOperator<AggregateBasedJpaEventStorageEngineConfiguration> configurer;
    private final TagResolver tagResolver;
    private final Map<TenantDescriptor, EventStore> eventStores = new ConcurrentHashMap<>();

    /**
     * Creates a new {@link JpaTenantEventSegmentFactory} with the given dependencies and default configuration.
     *
     * @param emfProvider    function that provides an {@link EntityManagerFactory} for each tenant
     * @param eventConverter the converter for serializing/deserializing events
     */
    public JpaTenantEventSegmentFactory(
            Function<TenantDescriptor, EntityManagerFactory> emfProvider,
            EventConverter eventConverter
    ) {
        this(emfProvider, eventConverter, c -> c, new AnnotationBasedTagResolver());
    }

    /**
     * Creates a new {@link JpaTenantEventSegmentFactory} with the given dependencies and custom configuration.
     *
     * @param emfProvider    function that provides an {@link EntityManagerFactory} for each tenant
     * @param eventConverter the converter for serializing/deserializing events
     * @param configurer     function to customize the {@link AggregateBasedJpaEventStorageEngineConfiguration}
     * @param tagResolver    the resolver for event tags
     */
    public JpaTenantEventSegmentFactory(
            Function<TenantDescriptor, EntityManagerFactory> emfProvider,
            EventConverter eventConverter,
            UnaryOperator<AggregateBasedJpaEventStorageEngineConfiguration> configurer,
            TagResolver tagResolver
    ) {
        this.emfProvider = Objects.requireNonNull(emfProvider,
                "EntityManagerFactory provider must not be null");
        this.eventConverter = Objects.requireNonNull(eventConverter,
                "EventConverter must not be null");
        this.configurer = Objects.requireNonNull(configurer,
                "Configurer must not be null");
        this.tagResolver = Objects.requireNonNull(tagResolver,
                "TagResolver must not be null");
    }

    @Override
    public EventStore apply(TenantDescriptor tenant) {
        return eventStores.computeIfAbsent(tenant, this::createEventStore);
    }

    private EventStore createEventStore(TenantDescriptor tenant) {
        logger.debug("Creating EventStore segment for tenant [{}]", tenant.tenantId());

        EntityManagerFactory emf = emfProvider.apply(tenant);

        AggregateBasedJpaEventStorageEngine storageEngine = new AggregateBasedJpaEventStorageEngine(
                new JpaTransactionalExecutorProvider(emf),
                eventConverter,
                configurer
        );

        EventStore eventStore = new StorageEngineBackedEventStore(
                storageEngine,
                new SimpleEventBus(),
                tagResolver
        );

        logger.info("Created EventStore segment for tenant [{}]", tenant.tenantId());
        return eventStore;
    }

    /**
     * Returns the number of event stores currently cached.
     * Primarily for testing purposes.
     *
     * @return the number of cached event stores
     */
    int eventStoreCount() {
        return eventStores.size();
    }
}
