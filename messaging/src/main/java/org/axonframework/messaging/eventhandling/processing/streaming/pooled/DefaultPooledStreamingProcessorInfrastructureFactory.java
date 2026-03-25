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

import org.axonframework.common.AxonThreadFactory;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.messaging.deadletter.SequencedDeadLetterProcessor;
import org.axonframework.messaging.deadletter.SequencedDeadLetterQueue;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.deadletter.CachingSequencedDeadLetterQueue;
import org.axonframework.messaging.eventhandling.deadletter.DeadLetterQueueConfiguration;
import org.axonframework.messaging.eventhandling.deadletter.DeadLetteringEventHandlingComponent;
import org.axonframework.messaging.eventhandling.interception.InterceptingEventHandlingComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.SegmentChangeListener;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.SequenceCachingEventHandlingComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.axonframework.common.configuration.ComponentDefinition.ofTypeAndName;

/**
 * Default implementation of {@link PooledStreamingProcessorInfrastructureFactory} that creates the standard
 * single-instance infrastructure for a {@link PooledStreamingEventProcessor}.
 * <p>
 * This is the factory used when no custom implementation is registered. It creates:
 * <ul>
 *     <li>The processor configuration with executor defaults</li>
 *     <li>Per-component dead letter queues (if DLQ is enabled)</li>
 *     <li>Token store</li>
 *     <li>Unit of work factory</li>
 *     <li>Event handling component decorations (interceptors + DLQ)</li>
 *     <li>The {@link PooledStreamingEventProcessor} itself</li>
 * </ul>
 *
 * @author Mateusz Nowak
 * @author Theo Emanuelsson
 * @since 5.2.0
 */
public class DefaultPooledStreamingProcessorInfrastructureFactory
        implements PooledStreamingProcessorInfrastructureFactory {

    @Override
    public void createInfrastructure(
            String processorName,
            Map<String, ComponentBuilder<EventHandlingComponent>> eventHandlingComponentBuilders,
            ComponentBuilder<PooledStreamingEventProcessorConfiguration> customizedProcessorConfigurationBuilder,
            ComponentRegistry registry
    ) {
        registerCustomizedConfiguration(processorName, eventHandlingComponentBuilders,
                                        customizedProcessorConfigurationBuilder, registry);
        registerDeadLetterQueues(processorName, eventHandlingComponentBuilders, registry);
        registerTokenStore(processorName, registry);
        registerUnitOfWorkFactory(processorName, registry);
        registerEventHandlingComponents(processorName, eventHandlingComponentBuilders, registry);
        registerEventProcessor(processorName, eventHandlingComponentBuilders, registry);
    }

    private void registerCustomizedConfiguration(
            String processorName,
            Map<String, ComponentBuilder<EventHandlingComponent>> eventHandlingComponentBuilders,
            ComponentBuilder<PooledStreamingEventProcessorConfiguration> customizedProcessorConfigurationBuilder,
            ComponentRegistry registry
    ) {
        registry.registerComponent(
                ComponentDefinition
                        .ofType(PooledStreamingEventProcessorConfiguration.class)
                        .withBuilder(cfg -> {
                            var configuration = customizedProcessorConfigurationBuilder.build(cfg);
                            configuration.workerExecutor(
                                    Optional.ofNullable(configuration.workerExecutor())
                                            .orElseGet(() -> defaultExecutor(4, "WorkPackage[" + processorName + "]"))
                            );
                            configuration.coordinatorExecutor(
                                    Optional.ofNullable(configuration.coordinatorExecutor())
                                            .orElseGet(() -> defaultExecutor(1, "Coordinator[" + processorName + "]"))
                            );
                            var dlqConfig = configuration.deadLetterQueue();
                            if (dlqConfig.isEnabled() && dlqConfig.cacheMaxSize() > 0) {
                                configuration.addSegmentChangeListener(SegmentChangeListener.onRelease(segment -> {
                                    var uow = configuration.unitOfWorkFactory().create();
                                    return uow.executeWithResult(context -> {
                                        for (String componentName : eventHandlingComponentBuilders.keySet()) {
                                            var dlq = (CachingSequencedDeadLetterQueue<?>) cfg.getComponent(
                                                    SequencedDeadLetterQueue.class,
                                                    processorComponentDlqName(processorName, componentName));
                                            dlq.invalidateCache(context.withResource(Segment.RESOURCE_KEY, segment));
                                        }
                                        return FutureUtils.emptyCompletedFuture();
                                    });
                                }));
                            }
                            return configuration;
                        }).onShutdown(Phase.LOCAL_MESSAGE_HANDLER_REGISTRATIONS, (cfg, processor) -> {
                            processor.workerExecutor().shutdown();
                            return FutureUtils.emptyCompletedFuture();
                        }).onShutdown(Phase.LOCAL_MESSAGE_HANDLER_REGISTRATIONS, (cfg, processor) -> {
                            processor.coordinatorExecutor().shutdown();
                            return FutureUtils.emptyCompletedFuture();
                        })
        );
    }

    @SuppressWarnings("unchecked")
    private void registerDeadLetterQueues(
            String processorName,
            Map<String, ComponentBuilder<EventHandlingComponent>> eventHandlingComponentBuilders,
            ComponentRegistry registry
    ) {
        for (String componentName : eventHandlingComponentBuilders.keySet()) {
            var dlqName = processorComponentDlqName(processorName, componentName);
            registry.registerComponent(
                    ComponentDefinition
                            .ofTypeAndName(SequencedDeadLetterQueue.class, dlqName)
                            .withBuilder(cfg -> {
                                DeadLetterQueueConfiguration dlqConfig =
                                        cfg.getComponent(PooledStreamingEventProcessorConfiguration.class)
                                           .deadLetterQueue();
                                if (dlqConfig.isEnabled()) {
                                    var underlyingDlq = dlqConfig.factory().create(dlqName, cfg);
                                    if (dlqConfig.cacheMaxSize() > 0) {
                                        return new CachingSequencedDeadLetterQueue<EventMessage>(
                                                underlyingDlq,
                                                dlqConfig.cacheMaxSize()
                                        );
                                    }
                                    return underlyingDlq;
                                }
                                return null;
                            })
            );
        }
    }

    private void registerTokenStore(String processorName, ComponentRegistry registry) {
        registry.registerComponent(
                ComponentDefinition
                        .ofTypeAndName(TokenStore.class, "TokenStore[" + processorName + "]")
                        .withBuilder(cfg -> cfg.getComponent(PooledStreamingEventProcessorConfiguration.class)
                                               .tokenStore())
        );
    }

    private void registerUnitOfWorkFactory(String processorName, ComponentRegistry registry) {
        registry.registerComponent(
                ComponentDefinition
                        .ofTypeAndName(UnitOfWorkFactory.class, "UnitOfWorkFactory[" + processorName + "]")
                        .withBuilder(cfg -> cfg.getComponent(PooledStreamingEventProcessorConfiguration.class)
                                               .unitOfWorkFactory())
        );
    }

    private void registerEventHandlingComponents(
            String processorName,
            Map<String, ComponentBuilder<EventHandlingComponent>> eventHandlingComponentBuilders,
            ComponentRegistry registry
    ) {
        for (var componentBuilderEntry : eventHandlingComponentBuilders.entrySet()) {
            var configuredComponentName = componentBuilderEntry.getKey();
            var componentBuilder = componentBuilderEntry.getValue();
            var componentName = processorEventHandlingComponentName(processorName, configuredComponentName);
            registry.registerComponent(EventHandlingComponent.class, componentName,
                                       cfg -> {
                                           var component = componentBuilder.build(cfg);
                                           return new SequenceCachingEventHandlingComponent(component);
                                       });
            registry.registerDecorator(EventHandlingComponent.class, componentName,
                                       InterceptingEventHandlingComponent.DECORATION_ORDER,
                                       (config, name, delegate) -> {
                                           var configuration =
                                                   config.getComponent(PooledStreamingEventProcessorConfiguration.class);
                                           return new InterceptingEventHandlingComponent(
                                                   configuration.interceptors(),
                                                   delegate
                                           );
                                       });
            registry.registerDecorator(EventHandlingComponent.class, componentName,
                                       DeadLetteringEventHandlingComponent.DECORATION_ORDER,
                                       (config, name, delegate) -> {
                                           var processorConfig = config.getComponent(PooledStreamingEventProcessorConfiguration.class);
                                           var dlqConfig = processorConfig.deadLetterQueue();
                                           if (!dlqConfig.isEnabled()) {
                                               return delegate;
                                           }
                                           var dlq = config.getComponent(
                                                   SequencedDeadLetterQueue.class,
                                                   processorComponentDlqName(processorName, configuredComponentName)
                                           );
                                           //noinspection unchecked
                                           return new DeadLetteringEventHandlingComponent(
                                                   delegate,
                                                   dlq,
                                                   dlqConfig.enqueuePolicy(),
                                                   processorConfig.unitOfWorkFactory(), dlqConfig.clearOnReset()
                                           );
                                       });
            registry.registerComponent(SequencedDeadLetterProcessor.class, componentName,
                                       cfg -> {
                                           var eventHandlingComponent = cfg.getComponent(
                                                   EventHandlingComponent.class, componentName
                                           );
                                           if (eventHandlingComponent instanceof SequencedDeadLetterProcessor<?> dlp) {
                                               return dlp;
                                           }
                                           return null;
                                       });
        }
    }

    private void registerEventProcessor(
            String processorName,
            Map<String, ComponentBuilder<EventHandlingComponent>> eventHandlingComponentBuilders,
            ComponentRegistry registry
    ) {
        var processorComponentDefinition = ComponentDefinition
                .ofTypeAndName(StreamingEventProcessor.class, processorName)
                .withBuilder(cfg -> new PooledStreamingEventProcessor(
                        processorName,
                        getEventHandlingComponents(processorName, eventHandlingComponentBuilders, cfg),
                        cfg.getComponent(PooledStreamingEventProcessorConfiguration.class)
                ))
                .onStart(Phase.INBOUND_EVENT_CONNECTORS, (cfg, component) -> {
                    return component.start();
                })
                .onShutdown(Phase.INBOUND_EVENT_CONNECTORS, (cfg, component) -> {
                    return component.shutdown();
                });

        registry.registerComponent(processorComponentDefinition);
    }

    private List<EventHandlingComponent> getEventHandlingComponents(
            String processorName,
            Map<String, ComponentBuilder<EventHandlingComponent>> eventHandlingComponentBuilders,
            org.axonframework.common.configuration.Configuration configuration
    ) {
        return eventHandlingComponentBuilders.keySet()
                .stream()
                .map(componentName -> configuration.getComponent(
                        EventHandlingComponent.class,
                        processorEventHandlingComponentName(processorName, componentName)
                ))
                .toList();
    }

    /**
     * Returns the component name for an event handling component within a processor.
     */
    static String processorEventHandlingComponentName(String processorName, String componentName) {
        return "EventHandlingComponent[" + processorName + "][" + componentName + "]";
    }

    /**
     * Returns the DLQ component name for an event handling component within a processor.
     */
    static String processorComponentDlqName(String processorName, String componentName) {
        return "DeadLetterQueue[" + processorName + "][" + componentName + "]";
    }

    private static ScheduledExecutorService defaultExecutor(int poolSize, String factoryName) {
        return Executors.newScheduledThreadPool(poolSize, new AxonThreadFactory(factoryName));
    }
}
