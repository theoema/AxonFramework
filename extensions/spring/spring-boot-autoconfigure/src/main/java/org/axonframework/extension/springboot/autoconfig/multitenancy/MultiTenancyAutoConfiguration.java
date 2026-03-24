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

import org.axonframework.extension.multitenancy.common.configuration.MultiTenancyConfigurationDefaults;
import org.axonframework.extension.spring.config.EventProcessorDefinition;
import org.axonframework.extension.spring.config.EventProcessorSettings;
import org.axonframework.extension.spring.config.MultiTenantProcessorModuleFactory;
import org.axonframework.extension.spring.config.ProcessorModuleFactory;
import org.axonframework.extension.springboot.autoconfig.EventProcessingAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

import java.util.List;

/**
 * Auto-configuration for multi-tenant Axon Framework event processing.
 * <p>
 * This configuration provides a {@link MultiTenantProcessorModuleFactory} that swaps standard
 * event processor modules with multi-tenant ones, giving each tenant its own
 * {@link org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor
 * PooledStreamingEventProcessor} with dedicated event source, token store, and executors.
 * <p>
 * The actual multi-tenant infrastructure components ({@code MultiTenantCommandBus},
 * {@code MultiTenantQueryBus}, {@code MultiTenantEventStore}) are created by the
 * {@link MultiTenancyConfigurationDefaults} SPI enhancer in the core module, activated when a
 * {@link org.axonframework.extension.multitenancy.common.TargetTenantResolver TargetTenantResolver}
 * is registered.
 * <p>
 * Users must provide the following beans:
 * <ul>
 *     <li>{@link org.axonframework.extension.multitenancy.common.TenantProvider TenantProvider}
 *         — defines which tenants exist</li>
 *     <li>{@link org.axonframework.extension.multitenancy.common.TargetTenantResolver TargetTenantResolver}
 *         — determines how to extract the tenant from a message</li>
 * </ul>
 * <p>
 * Multi-tenancy can be disabled by setting {@code axon.multi-tenancy.enabled=false}.
 *
 * @author Stefan Dragisic
 * @author Theo Emanuelsson
 * @since 4.6.0
 * @see MultiTenantProcessorModuleFactory
 * @see MultiTenancyConfigurationDefaults
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
}
