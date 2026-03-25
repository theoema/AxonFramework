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
package org.axonframework.extension.spring.config;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.annotation.Internal;
import org.axonframework.extension.multitenancy.messaging.eventhandling.processing.MultiTenantEventProcessorModule;
import org.axonframework.extension.spring.BeanDefinitionUtils;
import org.axonframework.messaging.eventhandling.configuration.EventHandlingComponentsConfigurer;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * A {@link ProcessorModuleFactory} that creates multi-tenant event processor modules for pooled streaming
 * processors.
 * <p>
 * This factory mirrors the logic of
 * {@link org.axonframework.extension.spring.config.DefaultProcessorModuleFactory DefaultProcessorModuleFactory}
 * for handler-to-processor assignment, but creates
 * {@link MultiTenantEventProcessorModule#pooledStreaming(String) multi-tenant pooled streaming modules} instead
 * of standard ones. Subscribing processors are created normally since they use the event bus subscription model
 * and don't need per-tenant handling.
 * <p>
 * This factory is registered as a Spring bean by {@link MultiTenancyAutoConfiguration}, replacing the default
 * factory via {@code @ConditionalOnMissingBean}.
 *
 * @author Theo Emanuelsson
 * @since 5.2.0
 * @see MultiTenantEventProcessorModule
 * @see org.axonframework.extension.spring.config.DefaultProcessorModuleFactory
 */
@Internal
public class MultiTenantProcessorModuleFactory implements ProcessorModuleFactory {

    private static final Logger logger = LoggerFactory.getLogger(MultiTenantProcessorModuleFactory.class);

    private final List<EventProcessorDefinition> eventProcessorDefinitions;
    private final Map<String, EventProcessorSettings> allSettings;

    /**
     * Creates a new factory with the given processor definitions and settings.
     *
     * @param eventProcessorDefinitions the list of processor definitions that define handler assignment rules
     * @param settings                  the map of processor settings, keyed by processor name
     */
    public MultiTenantProcessorModuleFactory(List<EventProcessorDefinition> eventProcessorDefinitions,
                                             Map<String, EventProcessorSettings> settings) {
        this.eventProcessorDefinitions = eventProcessorDefinitions;
        this.allSettings = settings;
    }

    @Override
    public Set<EventProcessorModule> buildProcessorModules(
            Set<EventProcessorDefinition.EventHandlerDescriptor> handlers
    ) {
        Set<EventProcessorModule> modules = new LinkedHashSet<>();

        var assignments = handlers.stream().collect(Collectors.groupingBy(this::assignedProcessor));

        for (EventProcessorDefinition definition : eventProcessorDefinitions) {
            if (!assignments.containsKey(definition.name())) {
                logger.warn("No handlers assigned to explicitly defined processor: {}", definition.name());
            }
        }

        assignments.forEach((processorName, beanDefs) -> {
            Function<EventHandlingComponentsConfigurer.RequiredComponentPhase, EventHandlingComponentsConfigurer.CompletePhase> componentRegistration =
                    (EventHandlingComponentsConfigurer.RequiredComponentPhase phase) -> {
                        EventHandlingComponentsConfigurer.ComponentsPhase resultOfRegistration = phase;
                        for (EventProcessorDefinition.EventHandlerDescriptor namedBeanDefinition : beanDefs) {
                            resultOfRegistration = resultOfRegistration.autodetected(
                                    namedBeanDefinition.beanName(),
                                    namedBeanDefinition.component()
                            );
                        }
                        return (EventHandlingComponentsConfigurer.CompletePhase) resultOfRegistration;
                    };

            var processorModuleName = "EventProcessor[" + processorName + "]";

            var settings = Optional.ofNullable(allSettings.get(processorName))
                                   .orElseGet(() -> allSettings.get(EventProcessorSettings.DEFAULT));
            var processorMode = definitionFor(processorName).map(EventProcessorDefinition::mode)
                                                            .orElse(settings.processorMode());
            var module = switch (processorMode) {
                case POOLED -> {
                    var moduleSettings = (EventProcessorSettings.PooledEventProcessorSettings) settings;
                    var customization = SpringCustomizations.pooledStreamingCustomizations(processorName, moduleSettings)
                                                            .andThen(customizeConfiguration(processorName));
                    yield MultiTenantEventProcessorModule
                            .pooledStreaming(processorModuleName)
                            .eventHandlingComponents(componentRegistration)
                            .customized(customization)
                            .build();
                }
                case SUBSCRIBING -> {
                    var moduleSettings = (EventProcessorSettings.SubscribingEventProcessorSettings) settings;
                    yield EventProcessorModule
                            .subscribing(processorModuleName)
                            .eventHandlingComponents(componentRegistration)
                            .customized(SpringCustomizations.subscribingCustomizations(processorName, moduleSettings)
                                                            .andThen(customizeConfiguration(processorName)))
                            .build();
                }
            };

            modules.add(module);
        });
        return modules;
    }

    @SuppressWarnings({"unchecked"})
    private <T extends EventProcessorConfiguration> UnaryOperator<T> customizeConfiguration(String processorName) {
        for (EventProcessorDefinition eventProcessorDefinition : eventProcessorDefinitions) {
            if (eventProcessorDefinition.name().equals(processorName)) {
                return c -> (T) eventProcessorDefinition.applySettings(c);
            }
        }
        return UnaryOperator.identity();
    }

    private String assignedProcessor(EventProcessorDefinition.EventHandlerDescriptor handler) {
        Set<String> matches = new HashSet<>();
        for (EventProcessorDefinition eventProcessorDefinition : eventProcessorDefinitions) {
            if (eventProcessorDefinition.matchesSelector(handler)) {
                matches.add(eventProcessorDefinition.name());
            }
        }
        if (matches.isEmpty()) {
            return BeanDefinitionUtils.extractPackageName(handler.beanDefinition());
        }
        if (matches.size() == 1) {
            return matches.iterator().next();
        }
        throw new AxonConfigurationException(
                "Handler [" + handler.beanName() + " (of type " + handler.beanDefinition().getBeanClassName()
                        + ")] matched with multiple processors selectors: " + matches);
    }

    private Optional<EventProcessorDefinition> definitionFor(String name) {
        for (EventProcessorDefinition eventProcessorDefinition : eventProcessorDefinitions) {
            if (eventProcessorDefinition.name().equals(name)) {
                return Optional.of(eventProcessorDefinition);
            }
        }
        return Optional.empty();
    }
}
