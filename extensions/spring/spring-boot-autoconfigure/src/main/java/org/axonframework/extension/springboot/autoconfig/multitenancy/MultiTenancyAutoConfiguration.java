/*
 * Copyright (c) 2010-2026. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.axonframework.extension.springboot.autoconfig.multitenancy;

import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.extension.multitenancy.common.TenantResolver;
import org.axonframework.extension.multitenancy.common.TenantResolverRegistry;
import org.axonframework.extension.multitenancy.common.configuration.MultiTenancyConfigurationDefaults;
import org.axonframework.extension.spring.config.EventProcessorDefinition;
import org.axonframework.extension.spring.config.EventProcessorSettings;
import org.axonframework.extension.spring.config.MultiTenantProcessorModuleFactory;
import org.axonframework.extension.spring.config.ProcessorModuleFactory;
import org.axonframework.extension.springboot.autoconfig.EventProcessingAutoConfiguration;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.Optional;

/**
 * Auto-configuration for multi-tenant Axon Framework event processing.
 * <p>
 * This configuration provides a {@link MultiTenantProcessorModuleFactory} that swaps standard
 * event processor modules with multi-tenant ones, giving each tenant its own
 * {@link org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor
 * PooledStreamingEventProcessor} with dedicated event source, token store, and executors.
 * <p>
 * When {@link TenantResolver} beans are present in the application context, they are automatically
 * discovered and registered on the {@link TenantResolverRegistry}. This follows the same pattern as
 * {@link org.axonframework.extension.springboot.autoconfig.ReactorAutoConfiguration ReactorAutoConfiguration}
 * for reactor dispatch interceptors.
 * <p>
 * The actual multi-tenant infrastructure components ({@code TenantRoutingCommandBus},
 * {@code TenantRoutingQueryBus}, {@code TenantRoutingEventStore}) are created by the
 * {@link MultiTenancyConfigurationDefaults} SPI enhancer in the core module, activated when a
 * {@link TenantResolver} is registered on the {@link TenantResolverRegistry}.
 * <p>
 * Users must provide the following beans:
 * <ul>
 *     <li>{@link org.axonframework.extension.multitenancy.common.TenantProvider TenantProvider}
 *         — defines which tenants exist</li>
 *     <li>{@link TenantResolver} — determines how to extract the tenant from a message</li>
 * </ul>
 * <p>
 * Multi-tenancy can be disabled by setting {@code axon.multi-tenancy.enabled=false}.
 *
 * @author Stefan Dragisic
 * @author Theo Emanuelsson
 * @since 4.6.0
 * @see MultiTenantProcessorModuleFactory
 * @see MultiTenancyConfigurationDefaults
 * @see TenantResolverRegistry
 */
@AutoConfiguration
@ConditionalOnProperty(value = "axon.multi-tenancy.enabled", matchIfMissing = true)
@AutoConfigureBefore(EventProcessingAutoConfiguration.class)
public class MultiTenancyAutoConfiguration {

    /**
     * Provides a multi-tenant {@link ProcessorModuleFactory} that creates multi-tenant pooled streaming
     * modules instead of standard ones.
     * <p>
     * This bean replaces the default {@link ProcessorModuleFactory} via {@code @ConditionalOnMissingBean},
     * ensuring Spring Boot users get multi-tenant event processors automatically.
     *
     * @param eventProcessorDefinitions the processor definitions from Spring context
     * @param eventProcessorSettings    the processor settings from application properties
     * @return the multi-tenant processor module factory
     */
    @Bean
    @ConditionalOnMissingBean
    public ProcessorModuleFactory processorModuleFactory(
            List<EventProcessorDefinition> eventProcessorDefinitions,
            EventProcessorSettings.MapWrapper eventProcessorSettings
    ) {
        return new MultiTenantProcessorModuleFactory(
                eventProcessorDefinitions, eventProcessorSettings.settings()
        );
    }

    /**
     * Creates a {@link DecoratorDefinition} that registers all discovered {@link TenantResolver}
     * beans on the {@link TenantResolverRegistry}.
     * <p>
     * This follows the same pattern as
     * {@link org.axonframework.extension.springboot.autoconfig.ReactorAutoConfiguration
     * ReactorAutoConfiguration} for reactor dispatch interceptors. Generic {@link Message} resolvers
     * are registered for all message types. Type-specific resolvers ({@link CommandMessage},
     * {@link EventMessage}, {@link QueryMessage}) are registered for their respective type only.
     *
     * @param resolvers        generic {@link Message} resolvers
     * @param commandResolvers {@link CommandMessage}-specific resolvers
     * @param eventResolvers   {@link EventMessage}-specific resolvers
     * @param queryResolvers   {@link QueryMessage}-specific resolvers
     * @return a decorator definition that registers the discovered resolvers
     */
    @Bean
    @ConditionalOnBean(TenantResolver.class)
    public DecoratorDefinition<TenantResolverRegistry, TenantResolverRegistry> tenantResolverEnhancer(
            Optional<List<TenantResolver<Message>>> resolvers,
            Optional<List<TenantResolver<? super CommandMessage>>> commandResolvers,
            Optional<List<TenantResolver<? super EventMessage>>> eventResolvers,
            Optional<List<TenantResolver<? super QueryMessage>>> queryResolvers
    ) {
        return DecoratorDefinition.forType(TenantResolverRegistry.class)
                                  .with((config, name, delegate) -> registerResolvers(
                                          delegate,
                                          resolvers,
                                          commandResolvers,
                                          eventResolvers,
                                          queryResolvers
                                  ));
    }

    private static TenantResolverRegistry registerResolvers(
            TenantResolverRegistry registry,
            Optional<List<TenantResolver<Message>>> resolvers,
            Optional<List<TenantResolver<? super CommandMessage>>> commandResolvers,
            Optional<List<TenantResolver<? super EventMessage>>> eventResolvers,
            Optional<List<TenantResolver<? super QueryMessage>>> queryResolvers
    ) {
        // Register generic resolvers (applies to all message types)
        resolvers.ifPresent(list -> list.forEach(
                resolver -> registry.registerResolver(c -> resolver)
        ));
        // Register type-specific resolvers (overrides generic for that type)
        commandResolvers.ifPresent(list -> list.forEach(
                resolver -> registry.registerCommandResolver(c -> resolver)
        ));
        eventResolvers.ifPresent(list -> list.forEach(
                resolver -> registry.registerEventResolver(c -> resolver)
        ));
        queryResolvers.ifPresent(list -> list.forEach(
                resolver -> registry.registerQueryResolver(c -> resolver)
        ));
        return registry;
    }
}
