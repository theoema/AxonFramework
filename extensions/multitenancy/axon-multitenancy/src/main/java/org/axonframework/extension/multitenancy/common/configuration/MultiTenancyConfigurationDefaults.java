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
package org.axonframework.extension.multitenancy.common.configuration;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.extension.multitenancy.common.DefaultTenantResolverRegistry;
import org.axonframework.extension.multitenancy.common.MultiTenantAwareComponent;
import org.axonframework.extension.multitenancy.common.TenantComponentFactory;
import org.axonframework.extension.multitenancy.common.TenantComponentRegistry;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.extension.multitenancy.common.TenantProvider;
import org.axonframework.extension.multitenancy.common.TenantResolver;
import org.axonframework.extension.multitenancy.common.TenantResolverRegistry;
import org.axonframework.extension.multitenancy.eventsourcing.eventstore.TenantRoutingEventStore;
import org.axonframework.extension.multitenancy.eventsourcing.eventstore.TenantEventSegmentFactory;
import org.axonframework.extension.multitenancy.eventsourcing.snapshot.TenantRoutingSnapshotStore;
import org.axonframework.extension.multitenancy.eventsourcing.snapshot.TenantSnapshotStoreSegmentFactory;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.extension.multitenancy.messaging.commandhandling.TenantRoutingCommandBus;
import org.axonframework.extension.multitenancy.messaging.commandhandling.TenantCommandSegmentFactory;
import org.axonframework.extension.multitenancy.messaging.eventhandling.processing.streaming.token.store.TenantTokenStoreFactory;
import org.axonframework.extension.multitenancy.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTenantTokenStoreFactory;
import org.axonframework.extension.multitenancy.messaging.core.annotation.TenantComponentResolverFactory;
import org.axonframework.extension.multitenancy.messaging.core.annotation.TenantDescriptorParameterResolverFactory;
import org.axonframework.extension.multitenancy.messaging.core.unitofwork.annotation.TenantAwareProcessingContextResolverFactory;
import org.axonframework.extension.multitenancy.messaging.queryhandling.TenantRoutingQueryBus;
import org.axonframework.extension.multitenancy.messaging.queryhandling.TenantQuerySegmentFactory;
import org.axonframework.eventsourcing.eventstore.AnnotationBasedTagResolver;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.InterceptingEventStore;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.annotation.MultiParameterResolverFactory;
import org.axonframework.messaging.core.configuration.reflection.ParameterResolverFactoryUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.core.MessageDispatchInterceptor;
import org.axonframework.messaging.core.interception.DispatchInterceptorRegistry;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.SimpleCommandBus;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.SimpleQueryBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * {@link ConfigurationEnhancer} that provides configuration for multi-tenancy components.
 * <p>
 * This enhancer is the <b>single source of truth</b> for multi-tenancy wiring. It provides default
 * segment factories for embedded (non-distributed) deployments and registers decorators that replace
 * standard infrastructure components ({@link CommandBus}, {@link QueryBus}, {@link EventStore},
 * {@link StreamingEventProcessor}) with their multi-tenant equivalents when a
 * {@link TenantResolver} is configured via the {@link TenantResolverRegistry}.
 * <p>
 * <b>Default Segment Factories:</b> For embedded deployments without Axon Server, this
 * enhancer provides default implementations:
 * <ul>
 *   <li>{@link TenantCommandSegmentFactory} - creates {@link SimpleCommandBus} per tenant</li>
 *   <li>{@link TenantQuerySegmentFactory} - creates {@link SimpleQueryBus} per tenant</li>
 *   <li>{@link TenantEventSegmentFactory} - creates in-memory {@link EventStore} per tenant</li>
 *   <li>{@link TenantTokenStoreFactory} - creates in-memory {@link TokenStore} per tenant</li>
 *   <li>{@link TenantEventProcessorSegmentFactory} - creates per-tenant
 *       {@link PooledStreamingEventProcessor} using the tenant's event store and token store</li>
 * </ul>
 * These defaults can be overridden by registering custom implementations.
 * <p>
 * <b>Decoration Order:</b> Multi-tenant decorators run BEFORE intercepting decorators
 * (e.g., {@code InterceptingCommandBus}). This means the decoration chain is:
 * <pre>
 *     User → InterceptingCommandBus → TenantRoutingCommandBus → TenantSegments
 * </pre>
 * This follows the standard Axon Framework pattern where interceptors wrap the outer bus,
 * and the multi-tenant bus handles routing to tenant-specific segments.
 * <p>
 * <b>Usage:</b> Users configure multi-tenancy via the {@link TenantResolverRegistry}:
 * <pre>{@code
 * var configurer = MessagingConfigurer.create();
 * configurer.componentRegistry(cr -> {
 *     cr.registerComponent(TenantProvider.class, config -> myProvider);
 *     cr.registerDecorator(TenantResolverRegistry.class, 0,
 *             (config, name, delegate) -> delegate.registerResolver(c -> myResolver));
 * });
 * }</pre>
 *
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @author Theo Emanuelsson
 * @since 4.6.0
 */
public class MultiTenancyConfigurationDefaults implements ConfigurationEnhancer {

    private static final Logger logger = LoggerFactory.getLogger(MultiTenancyConfigurationDefaults.class);


    /**
     * The order of {@code this} enhancer compared to others.
     * <p>
     * Using {@code Integer.MAX_VALUE - 1} ensures multi-tenancy configuration runs after
     * most other enhancers but before the final defaults.
     */
    public static final int ENHANCER_ORDER = Integer.MAX_VALUE - 1;

    @Override
    public int order() {
        return ENHANCER_ORDER;
    }

    @Override
    public void enhance(ComponentRegistry componentRegistry) {
        // Register the TenantResolverRegistry so users can register resolvers
        componentRegistry.registerIfNotPresent(
                TenantResolverRegistry.class,
                config -> new DefaultTenantResolverRegistry()
        );

        // Register default segment factories for embedded mode (can be overridden by Axon Server enhancer)
        componentRegistry.registerIfNotPresent(
                TenantCommandSegmentFactory.class,
                config -> tenant -> defaultCommandBusSegment(config, tenant)
        );
        componentRegistry.registerIfNotPresent(
                TenantQuerySegmentFactory.class,
                config -> tenant -> defaultQueryBusSegment(config, tenant)
        );
        componentRegistry.registerIfNotPresent(
                TenantEventSegmentFactory.class,
                config -> tenant -> defaultEventStoreSegment(config, tenant)
        );
        componentRegistry.registerIfNotPresent(
                TenantTokenStoreFactory.class,
                config -> new InMemoryTenantTokenStoreFactory()
        );
        componentRegistry.registerIfNotPresent(
                TenantSnapshotStoreSegmentFactory.class,
                config -> tenant -> new InMemorySnapshotStore()
        );

        // Register decorator to replace CommandBus with TenantRoutingCommandBus
        componentRegistry.registerDecorator(
                CommandBus.class,
                TenantRoutingCommandBus.DECORATION_ORDER,
                (config, name, delegate) -> createTenantRoutingCommandBus(config, delegate)
        );

        // Register decorator to replace QueryBus with TenantRoutingQueryBus
        componentRegistry.registerDecorator(
                QueryBus.class,
                TenantRoutingQueryBus.DECORATION_ORDER,
                (config, name, delegate) -> createTenantRoutingQueryBus(config, delegate)
        );

        // Register decorator to replace EventStore with TenantRoutingEventStore
        componentRegistry.registerDecorator(
                EventStore.class,
                TenantRoutingEventStore.DECORATION_ORDER,
                (config, name, delegate) -> createTenantRoutingEventStore(config, delegate)
        );

        // Register decorator to replace SnapshotStore with TenantRoutingSnapshotStore
        componentRegistry.registerDecorator(
                SnapshotStore.class,
                TenantRoutingSnapshotStore.DECORATION_ORDER,
                (config, name, delegate) -> createTenantRoutingSnapshotStore(config, delegate)
        );

        // Register tenant component parameter resolvers.
        // Discovers all TenantComponentFactory beans at resolution time and registers them
        // for injection into message handlers.
        ParameterResolverFactoryUtils.registerToComponentRegistry(
                componentRegistry,
                config -> createTenantComponentResolverFactory(config)
        );
    }

    @SuppressWarnings("unchecked")
    private MultiParameterResolverFactory createTenantComponentResolverFactory(Configuration config) {
        TenantResolverRegistry registry = config.getComponent(TenantResolverRegistry.class);
        if (!registry.hasResolver()) {
            return MultiParameterResolverFactory.ordered();
        }

        TenantResolver<Message> tenantResolver = registry.resolver(config);
        if (tenantResolver == null) {
            // Only type-specific resolvers are configured, use command resolver as general fallback
            // since component resolution happens in message handling context
            return MultiParameterResolverFactory.ordered();
        }

        TenantComponentResolverFactory componentFactory = new TenantComponentResolverFactory(tenantResolver);

        TenantProvider tenantProvider = config.getOptionalComponent(TenantProvider.class).orElse(null);

        Map<String, TenantComponentFactory> factories = config.getComponents(TenantComponentFactory.class);
        for (var entry : factories.entrySet()) {
            TenantComponentFactory<Object> factory = entry.getValue();
            Class<?> componentType = resolveComponentType(factory);
            if (componentType != null) {
                TenantComponentRegistry<?> componentRegistry = componentFactory.registerComponent(
                        (Class<Object>) componentType, factory
                );
                if (tenantProvider != null) {
                    tenantProvider.subscribe(componentRegistry);
                    tenantProvider.getTenants().forEach(componentRegistry::registerTenant);
                }
                logger.debug("Registered tenant component factory for type [{}]", componentType.getName());
            }
        }

        TenantAwareProcessingContextResolverFactory contextFactory =
                new TenantAwareProcessingContextResolverFactory(componentFactory, tenantResolver);

        TenantDescriptorParameterResolverFactory descriptorFactory =
                new TenantDescriptorParameterResolverFactory(tenantResolver);

        return MultiParameterResolverFactory.ordered(componentFactory, contextFactory, descriptorFactory);
    }

    private Class<?> resolveComponentType(TenantComponentFactory<?> factory) {
        Class<?> declared = factory.componentType();
        if (declared != null) {
            return declared;
        }
        for (java.lang.reflect.Type iface : factory.getClass().getGenericInterfaces()) {
            if (iface instanceof java.lang.reflect.ParameterizedType pt
                && TenantComponentFactory.class.isAssignableFrom((Class<?>) pt.getRawType())) {
                java.lang.reflect.Type arg = pt.getActualTypeArguments()[0];
                if (arg instanceof Class<?> cls) {
                    return cls;
                }
            }
        }
        return null;
    }

    private CommandBus createTenantRoutingCommandBus(Configuration config, CommandBus delegate) {
        TenantResolverRegistry resolverRegistry = config.getComponent(TenantResolverRegistry.class);
        if (!config.hasComponent(TenantCommandSegmentFactory.class) || !resolverRegistry.hasResolver()) {
            return delegate;
        }

        TenantResolver<Message> resolver = resolverRegistry.commandResolver(config);
        if (resolver == null) {
            return delegate;
        }

        TenantCommandSegmentFactory segmentFactory = config.getComponent(TenantCommandSegmentFactory.class);
        TenantRoutingCommandBus multiTenantBus = new TenantRoutingCommandBus(segmentFactory, resolver);

        registerTenantsIfProviderAvailable(config, multiTenantBus);
        return multiTenantBus;
    }

    private QueryBus createTenantRoutingQueryBus(Configuration config, QueryBus delegate) {
        TenantResolverRegistry resolverRegistry = config.getComponent(TenantResolverRegistry.class);
        if (!config.hasComponent(TenantQuerySegmentFactory.class) || !resolverRegistry.hasResolver()) {
            return delegate;
        }

        TenantResolver<Message> resolver = resolverRegistry.queryResolver(config);
        if (resolver == null) {
            return delegate;
        }

        TenantQuerySegmentFactory segmentFactory = config.getComponent(TenantQuerySegmentFactory.class);
        TenantRoutingQueryBus multiTenantBus = new TenantRoutingQueryBus(segmentFactory, resolver);

        registerTenantsIfProviderAvailable(config, multiTenantBus);
        return multiTenantBus;
    }

    private EventStore createTenantRoutingEventStore(Configuration config, EventStore delegate) {
        TenantResolverRegistry resolverRegistry = config.getComponent(TenantResolverRegistry.class);
        if (!config.hasComponent(TenantEventSegmentFactory.class) || !resolverRegistry.hasResolver()) {
            return delegate;
        }

        // EventStore resolves from the processing context's message (usually a command),
        // not from the event itself, so we use the general resolver.
        TenantResolver<Message> resolver = resolverRegistry.resolver(config);
        if (resolver == null) {
            return delegate;
        }

        TenantEventSegmentFactory segmentFactory = config.getComponent(TenantEventSegmentFactory.class);
        TenantRoutingEventStore multiTenantStore = new TenantRoutingEventStore(segmentFactory, resolver);

        registerTenantsIfProviderAvailable(config, multiTenantStore);
        return multiTenantStore;
    }

    private SnapshotStore createTenantRoutingSnapshotStore(Configuration config, SnapshotStore delegate) {
        TenantResolverRegistry resolverRegistry = config.getComponent(TenantResolverRegistry.class);
        if (!config.hasComponent(TenantSnapshotStoreSegmentFactory.class) || !resolverRegistry.hasResolver()) {
            return delegate;
        }

        // SnapshotStore resolves from the processing context's message (usually a command),
        // not from events, so we use the general resolver.
        TenantResolver<Message> resolver = resolverRegistry.resolver(config);
        if (resolver == null) {
            return delegate;
        }

        TenantSnapshotStoreSegmentFactory segmentFactory = config.getComponent(TenantSnapshotStoreSegmentFactory.class);
        TenantRoutingSnapshotStore multiTenantStore = new TenantRoutingSnapshotStore(segmentFactory, resolver);

        registerTenantsIfProviderAvailable(config, multiTenantStore);
        return multiTenantStore;
    }

    private void registerTenantsIfProviderAvailable(Configuration config, MultiTenantAwareComponent component) {
        if (config.hasComponent(TenantProvider.class)) {
            TenantProvider tenantProvider = config.getComponent(TenantProvider.class);
            tenantProvider.subscribe(component);
            tenantProvider.getTenants().forEach(component::registerTenant);
        }
    }

    private CommandBus defaultCommandBusSegment(Configuration config, TenantDescriptor tenant) {
        return new SimpleCommandBus(config.getComponent(UnitOfWorkFactory.class));
    }

    private QueryBus defaultQueryBusSegment(Configuration config, TenantDescriptor tenant) {
        return new SimpleQueryBus(config.getComponent(UnitOfWorkFactory.class));
    }

    private EventStore defaultEventStoreSegment(Configuration config, TenantDescriptor tenant) {
        EventStore rawEventStore = new StorageEngineBackedEventStore(
                new InMemoryEventStorageEngine(),
                new SimpleEventBus(),
                new AnnotationBasedTagResolver()
        );

        List<MessageDispatchInterceptor<? super EventMessage>> dispatchInterceptors =
                config.getComponent(DispatchInterceptorRegistry.class).eventInterceptors(config, EventStore.class, null);

        return dispatchInterceptors.isEmpty()
                ? rawEventStore
                : new InterceptingEventStore(rawEventStore, dispatchInterceptors);
    }

}
