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
package org.axonframework.extension.multitenancy.axonserver.configuration;

import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.axonserver.connector.AxonServerConnectionManager;
import org.axonframework.axonserver.connector.command.AxonServerCommandBusConnector;
import org.axonframework.axonserver.connector.event.AxonServerEventStorageEngine;
import org.axonframework.axonserver.connector.event.AxonServerEventStorageEngineFactory;
import org.axonframework.axonserver.connector.query.AxonServerQueryBusConnector;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.eventsourcing.eventstore.AnnotationBasedTagResolver;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.axonframework.extension.multitenancy.axonserver.AxonServerTenantProvider;
import org.axonframework.extension.multitenancy.common.TenantConnectPredicate;
import org.axonframework.extension.multitenancy.common.TenantProvider;
import org.axonframework.extension.multitenancy.common.configuration.MultiTenancyConfigurationDefaults;
import org.axonframework.extension.multitenancy.eventsourcing.eventstore.TenantEventSegmentFactory;
import org.axonframework.extension.multitenancy.messaging.commandhandling.TenantCommandSegmentFactory;
import org.axonframework.extension.multitenancy.messaging.queryhandling.TenantQuerySegmentFactory;
import org.axonframework.messaging.commandhandling.SimpleCommandBus;
import org.axonframework.messaging.commandhandling.distributed.DistributedCommandBus;
import org.axonframework.messaging.commandhandling.distributed.DistributedCommandBusConfiguration;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.queryhandling.SimpleQueryBus;
import org.axonframework.messaging.queryhandling.distributed.DistributedQueryBus;
import org.axonframework.messaging.queryhandling.distributed.DistributedQueryBusConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ConfigurationEnhancer} that provides Axon Server-specific multi-tenancy segment factories.
 * <p>
 * When the Axon Server connector is on the classpath, this enhancer overrides the default embedded
 * segment factories with Axon Server-backed versions that create per-tenant distributed buses and
 * event stores, each connected to the tenant's Axon Server context.
 * <p>
 * For each tenant, the segment factories create:
 * <ul>
 *     <li>A {@link DistributedCommandBus} backed by a tenant-specific
 *         {@link AxonServerCommandBusConnector}</li>
 *     <li>A {@link DistributedQueryBus} backed by a tenant-specific
 *         {@link AxonServerQueryBusConnector}</li>
 *     <li>An {@link EventStore} backed by a
 *         tenant-specific {@link AxonServerEventStorageEngine}</li>
 * </ul>
 *
 * @author Stefan Dragisic
 * @author Theo Emanuelsson
 * @since 4.6.0
 * @see AxonServerTenantProvider
 * @see MultiTenancyConfigurationDefaults
 */
public class MultiTenantAxonServerConfigurationDefaults implements ConfigurationEnhancer {

    private static final Logger logger = LoggerFactory.getLogger(MultiTenantAxonServerConfigurationDefaults.class);

    /**
     * Runs just before {@link MultiTenancyConfigurationDefaults} so that Axon Server segment factories
     * take precedence over the embedded defaults via {@code registerIfNotPresent}.
     */
    public static final int ENHANCER_ORDER = MultiTenancyConfigurationDefaults.ENHANCER_ORDER - 5;

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerIfNotPresent(
                TenantConnectPredicate.class,
                config -> tenant -> true
        );
        registry.registerIfNotPresent(tenantProviderDefinition(), SearchScope.ALL);

        registry.registerIfNotPresent(
                TenantCommandSegmentFactory.class,
                config -> config.getOptionalComponent(AxonServerConnectionManager.class)
                        .map(connectionManager -> {
                            var asConfig = config.getComponent(AxonServerConfiguration.class);
                            var uowFactory = config.getComponent(UnitOfWorkFactory.class);
                            TenantCommandSegmentFactory factory = tenant -> {
                                logger.debug("Creating DistributedCommandBus for tenant [{}]", tenant.tenantId());
                                var connection = connectionManager.getConnection(tenant.tenantId());
                                var connector = new AxonServerCommandBusConnector(connection, asConfig);
                                connector.start();
                                return new DistributedCommandBus(
                                        new SimpleCommandBus(uowFactory),
                                        connector,
                                        DistributedCommandBusConfiguration.DEFAULT
                                );
                            };
                            return factory;
                        })
                        .orElse(null)
        );

        registry.registerIfNotPresent(
                TenantQuerySegmentFactory.class,
                config -> config.getOptionalComponent(AxonServerConnectionManager.class)
                        .map(connectionManager -> {
                            var asConfig = config.getComponent(AxonServerConfiguration.class);
                            var uowFactory = config.getComponent(UnitOfWorkFactory.class);
                            TenantQuerySegmentFactory factory = tenant -> {
                                logger.debug("Creating DistributedQueryBus for tenant [{}]", tenant.tenantId());
                                var connection = connectionManager.getConnection(tenant.tenantId());
                                var connector = new AxonServerQueryBusConnector(connection, asConfig);
                                connector.start();
                                return new DistributedQueryBus(
                                        new SimpleQueryBus(uowFactory),
                                        connector,
                                        new DistributedQueryBusConfiguration()
                                );
                            };
                            return factory;
                        })
                        .orElse(null)
        );

        registry.registerIfNotPresent(
                TenantEventSegmentFactory.class,
                config -> config.getOptionalComponent(AxonServerConnectionManager.class)
                        .map(connectionManager -> {
                            var tagResolver = config.getComponent(
                                    TagResolver.class, AnnotationBasedTagResolver::new
                            );
                            TenantEventSegmentFactory factory = tenant -> {
                                logger.debug("Creating EventStore for tenant [{}]", tenant.tenantId());
                                var engine = AxonServerEventStorageEngineFactory.constructForContext(
                                        tenant.tenantId(), config
                                );
                                return new StorageEngineBackedEventStore(
                                        engine, new SimpleEventBus(), tagResolver
                                );
                            };
                            return factory;
                        })
                        .orElse(null)
        );
    }

    private static ComponentDefinition<TenantProvider> tenantProviderDefinition() {
        return ComponentDefinition.ofType(TenantProvider.class)
                .withBuilder(config ->
                        config.getOptionalComponent(AxonServerConnectionManager.class)
                              .map(connectionManager -> new AxonServerTenantProvider(
                                      connectionManager,
                                      config.getComponent(
                                              TenantConnectPredicate.class,
                                              () -> tenant -> true
                                      )
                              ))
                              .orElse(null)
                )
                .onStart(Phase.INSTRUCTION_COMPONENTS + 10,
                         provider -> ((AxonServerTenantProvider) provider).start())
                .onShutdown(Phase.INSTRUCTION_COMPONENTS + 10,
                            provider -> ((AxonServerTenantProvider) provider).shutdown());
    }

    @Override
    public int order() {
        return ENHANCER_ORDER;
    }
}
