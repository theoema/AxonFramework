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

package org.axonframework.messaging.eventhandling.processing.streaming.pooled;

import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;

import java.util.Map;

/**
 * Factory for creating the infrastructure of a {@link PooledStreamingEventProcessor}.
 * <p>
 * The {@link PooledStreamingEventProcessorModule} delegates infrastructure creation to this factory
 * during its {@code build()} phase. The factory receives the processor specification (name, handler
 * builders, configuration) and registers all infrastructure components in the module's
 * {@link ComponentRegistry}.
 * <p>
 * The default implementation ({@link DefaultPooledStreamingProcessorInfrastructureFactory}) creates
 * the standard single-instance infrastructure: processor, token store, DLQ, and event handling
 * component decorations.
 * <p>
 * Extensions can register a custom implementation as a component in the parent configuration to
 * change how infrastructure is constructed. For example, a multi-tenancy extension can provide a
 * factory that creates per-tenant infrastructure.
 *
 * @author Theo Emanuelsson
 * @since 5.2.0
 * @see PooledStreamingEventProcessorModule
 * @see DefaultPooledStreamingProcessorInfrastructureFactory
 */
@FunctionalInterface
public interface PooledStreamingProcessorInfrastructureFactory {

    /**
     * Creates and registers all infrastructure components for a pooled streaming event processor.
     *
     * @param processorName                           the processor name
     * @param eventHandlingComponentBuilders          the user-provided handler builders, keyed by component name
     * @param customizedProcessorConfigurationBuilder the configuration builder with all customizations applied
     * @param registry                                the module's component registry to register infrastructure in
     */
    void createInfrastructure(
            String processorName,
            Map<String, ComponentBuilder<EventHandlingComponent>> eventHandlingComponentBuilders,
            ComponentBuilder<PooledStreamingEventProcessorConfiguration> customizedProcessorConfigurationBuilder,
            ComponentRegistry registry
    );
}
