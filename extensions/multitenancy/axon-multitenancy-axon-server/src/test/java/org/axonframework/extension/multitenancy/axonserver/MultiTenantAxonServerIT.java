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

package org.axonframework.extension.multitenancy.axonserver;

import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.EventSourcedEntityFactory;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.extension.multitenancy.common.MetadataBasedTenantResolver;
import org.axonframework.extension.multitenancy.common.SimpleTenantProvider;
import org.axonframework.extension.multitenancy.common.TenantResolverRegistry;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.extension.multitenancy.common.TenantProvider;
import org.axonframework.extension.multitenancy.messaging.eventhandling.processing.MultiTenantEventProcessor;
import org.axonframework.extension.multitenancy.messaging.eventhandling.processing.MultiTenantEventProcessorModule;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.correlation.SimpleCorrelationDataProvider;
import org.axonframework.messaging.core.sequencing.SequentialPolicy;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.modelling.EntityEvolver;
import org.axonframework.modelling.StateManager;
import org.axonframework.modelling.annotation.TargetEntityId;
import org.axonframework.test.server.AxonServerContainer;
import org.axonframework.test.server.AxonServerContainerUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Axon Server integration test for the multi-tenancy extension using a single tenant ("default").
 * <p>
 * This test uses the "default" context that ships with every Axon Server instance. Multi-context
 * support is an Axon Server Enterprise feature and is <b>not</b> available in dev-mode, so this
 * test is limited to a single tenant to stay compatible with the standard (non-enterprise) image.
 * <p>
 * Cross-tenant isolation scenarios (two or more tenants with separate contexts) are covered by
 * {@link org.axonframework.extension.multitenancy.MultiTenantEmbeddedIT} which uses embedded,
 * in-memory infrastructure and does not require Axon Server Enterprise.
 * <p>
 * The SPI-loaded {@link org.axonframework.extension.multitenancy.axonserver.configuration.MultiTenantAxonServerConfigurationDefaults}
 * enhancer transparently replaces CommandBus, EventStore, and StreamingEventProcessor with
 * Axon Server-backed multi-tenant versions.
 *
 * @author Theo Emanuelsson
 * @since 5.2.0
 */
@Timeout(60)
@Testcontainers
class MultiTenantAxonServerIT {

    private static final Logger logger = LoggerFactory.getLogger(MultiTenantAxonServerIT.class);

    // The tenantId is "default" because the Axon Server test container only provides a single context.
    // Creating additional contexts requires Axon Server Enterprise, which is not available in dev mode.
    private static final TenantDescriptor TENANT_DEFAULT = TenantDescriptor.tenantWithId("default");

    private static final GenericCommandResultMessage SUCCESSFUL_COMMAND_RESULT =
            new GenericCommandResultMessage(new MessageType("empty"), "successful");

    private static final AxonServerContainer container =
            new AxonServerContainer("docker.axoniq.io/axoniq/axonserver:2025.2.0")
                    .withAxonServerHostname("localhost")
                    .withDevMode(true)
                    .withReuse(true)
                    .withDcbContext(true);

    // Recording projection -- collects processed course IDs
    static final List<String> processedEvents = new CopyOnWriteArrayList<>();

    private AxonConfiguration configuration;
    private CommandGateway commandGateway;

    @BeforeAll
    static void beforeAll() {
        container.start();
        logger.info(
                "Using Axon Server for multi-tenancy integration test. UI is available at http://localhost:{}",
                container.getHttpPort()
        );
        // No context creation needed -- the "default" context exists out of the box.
    }

    @BeforeEach
    void setUp() throws IOException {
        processedEvents.clear();

        // given -- purge events from the default context
        logger.info("Purging events from Axon Server default context.");
        AxonServerContainerUtils.purgeEventsFromAxonServer(
                container.getHost(), container.getHttpPort(),
                "default", AxonServerContainerUtils.DCB_CONTEXT
        );

        // given -- single tenant mapped to the default AS context
        var tenantProvider = new SimpleTenantProvider();
        tenantProvider.addTenant(TENANT_DEFAULT);

        // given -- entity module with explicit evolver and criteria resolver
        var courseEntity = EventSourcedEntityModule
                .declarative(String.class, CourseState.class)
                .messagingModel((c, b) -> b.entityEvolver(courseEvolver()).build())
                .entityFactory(c -> EventSourcedEntityFactory.fromIdentifier(CourseState::new))
                .criteriaResolver(c -> (courseId, ctx) -> EventCriteria.havingTags("courseId", courseId))
                .build();

        // given -- programmatic command handler
        var commandHandlingModule = CommandHandlingModule
                .named("CourseCommands")
                .commandHandlers()
                .commandHandler(
                        new QualifiedName(CreateCourse.class),
                        (command, context) -> {
                            var ctx = Message.addToContext(context, command);
                            var payload = command.payloadAs(CreateCourse.class);
                            var stateManager = ctx.component(StateManager.class);
                            var state = stateManager.loadEntity(
                                    CourseState.class, payload.courseId(), ctx
                            ).join();
                            if (!state.created) {
                                var eventAppender = EventAppender.forContext(ctx);
                                eventAppender.append(new CourseCreated(payload.courseId()));
                            }
                            return MessageStream.just(SUCCESSFUL_COMMAND_RESULT).cast();
                        }
                )
                .build();

        // given -- programmatic event handling component for projection
        var courseProjectionComponent =
                SimpleEventHandlingComponent.create("courseProjection", SequentialPolicy.INSTANCE);
        courseProjectionComponent.subscribe(
                new QualifiedName(CourseCreated.class),
                (event, context) -> {
                    var payload = event.payloadAs(CourseCreated.class);
                    processedEvents.add(payload.courseId());
                    return MessageStream.empty();
                }
        );

        // Multi-tenant event processor module for the projection
        var projectionProcessor = MultiTenantEventProcessorModule
                .pooledStreaming("CourseProjection")
                .eventHandlingComponents(c -> c.declarative(
                        "courseProjection",
                        cfg -> courseProjectionComponent
                ))
                .notCustomized();

        // given -- Axon configuration with AS connection and multi-tenancy via SPI enhancers
        var configurer = EventSourcingConfigurer.create();

        var axonServerConfiguration = new AxonServerConfiguration();
        axonServerConfiguration.setServers(container.getHost() + ":" + container.getGrpcPort());

        configurer.componentRegistry(cr -> {
            cr.registerComponent(AxonServerConfiguration.class, config -> axonServerConfiguration);
            cr.registerComponent(TenantProvider.class, config -> tenantProvider);
            cr.registerDecorator(
                    TenantResolverRegistry.class, 0,
                    (config, name, delegate) -> delegate.registerResolver(c -> new MetadataBasedTenantResolver())
            );
        });

        // Register tenant ID as correlation data so events carry the tenant metadata
        configurer.messaging(m -> m.registerCorrelationDataProvider(
                config -> new SimpleCorrelationDataProvider("tenantId")
        ));

        configurer.componentRegistry(cr -> cr.registerModule(courseEntity));
        configurer.registerCommandHandlingModule(commandHandlingModule);

        configurer.messaging(m -> m.eventProcessing(ep -> ep
                .pooledStreaming(ps -> ps.processor(projectionProcessor))
        ));

        configuration = configurer.build();
        configuration.start();
        commandGateway = configuration.getComponent(CommandGateway.class);
    }

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Nested
    class Connectivity {

        @Test
        void configurationStartsSuccessfullyWithAxonServerEnhancer() {
            // then -- configuration started without errors in setUp; command gateway is available
            assertThat(commandGateway).isNotNull();
        }
    }

    @Nested
    class CommandAndEventProcessing {

        @Test
        void commandIsHandledAndProjectionProcessesTheEvent() {
            // when
            sendCommand("course-1");

            // then
            await().atMost(15, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(processedEvents).contains("course-1"));
        }

        @Test
        void multipleCommandsAreProcessedByProjection() {
            // when
            sendCommand("course-a1");
            sendCommand("course-a2");
            sendCommand("course-a3");

            // then
            await().atMost(15, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(processedEvents)
                           .containsExactlyInAnyOrder("course-a1", "course-a2", "course-a3"));
        }
    }

    @Nested
    class EventSourcingIdempotency {

        @Test
        void duplicateCommandForSameEntityDoesNotProduceDuplicateEvent() {
            // when -- send the same command twice for the same entity ID;
            //        the handler checks if (!state.created) so the second should produce no event
            sendCommand("idempotent-1");
            sendCommand("idempotent-1");

            // then -- at least one event is processed; the second command completes without error
            await().atMost(15, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(processedEvents).contains("idempotent-1"));

            // then -- the idempotency guard prevented a duplicate event
            await().during(500, TimeUnit.MILLISECONDS)
                   .atMost(2, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(processedEvents.stream()
                           .filter("idempotent-1"::equals)
                           .count()).isEqualTo(1));
        }
    }

    @Nested
    class ProcessorLifecycle {

        @Test
        void courseProjectionProcessorIsMultiTenantAndRunning() {
            // when
            Map<String, EventProcessor> processors = configuration.getComponents(EventProcessor.class);

            // then -- the "CourseProjection" processor exists and is a MultiTenantEventProcessor
            assertThat(processors).containsKey("CourseProjection");
            EventProcessor processor = processors.get("CourseProjection");
            assertThat(processor).isInstanceOf(MultiTenantEventProcessor.class);

            MultiTenantEventProcessor multiTenantProcessor = (MultiTenantEventProcessor) processor;
            assertThat(multiTenantProcessor.isRunning()).isTrue();
            // Single tenant ("default") means exactly 1 tenant segment
            assertThat(multiTenantProcessor.tenantSegments()).hasSize(1);
        }
    }

    // --- Helpers ---

    private void sendCommand(String courseId) {
        commandGateway.send(
                new CreateCourse(courseId),
                Metadata.with("tenantId", TENANT_DEFAULT.tenantId()),
                null
        ).getResultMessage().orTimeout(10, TimeUnit.SECONDS).join();
    }

    // --- Domain ---

    record CreateCourse(@TargetEntityId String courseId) {
    }

    record CourseCreated(@EventTag String courseId) {
    }

    static class CourseState {

        boolean created;

        CourseState(String id) {
            this.created = false;
        }
    }

    private static EntityEvolver<CourseState> courseEvolver() {
        return (entity, event, context) -> {
            if (event.type().qualifiedName().equals(new QualifiedName(CourseCreated.class))) {
                entity.created = true;
            }
            return entity;
        };
    }
}
