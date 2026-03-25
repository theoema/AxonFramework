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

package org.axonframework.extension.multitenancy;

import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.extension.multitenancy.common.configuration.MultiTenancyConfigurationDefaults;
import org.axonframework.eventsourcing.EventSourcedEntityFactory;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.extension.multitenancy.common.MetadataBasedTenantResolver;
import org.axonframework.extension.multitenancy.common.SimpleTenantProvider;
import org.axonframework.extension.multitenancy.common.TenantResolverRegistry;
import org.axonframework.extension.multitenancy.common.TenantComponentFactory;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.extension.multitenancy.common.TenantProvider;
import org.axonframework.extension.multitenancy.eventsourcing.snapshot.TenantRoutingSnapshotStore;
import org.axonframework.extension.multitenancy.messaging.eventhandling.processing.MultiTenantEventProcessor;
import org.axonframework.extension.multitenancy.messaging.eventhandling.processing.MultiTenantEventProcessorModule;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.GenericMessage;
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
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;
import org.axonframework.messaging.queryhandling.configuration.QueryHandlingModule;
import org.axonframework.modelling.EntityEvolver;
import org.axonframework.modelling.StateManager;
import org.axonframework.modelling.annotation.TargetEntityId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Embedded integration test for the multi-tenancy extension.
 * <p>
 * Verifies the full multi-tenancy flow with in-memory infrastructure -- no Axon Server required.
 * The SPI-loaded {@link MultiTenancyConfigurationDefaults}
 * enhancer transparently replaces CommandBus, EventStore, and StreamingEventProcessor with
 * multi-tenant versions. No AS connector is on the classpath so no AS enhancers load.
 *
 * @author Theo Emanuelsson
 * @since 5.2.0
 */
@Timeout(30)
class MultiTenantEmbeddedIT {

    private static final TenantDescriptor TENANT_A = TenantDescriptor.tenantWithId("tenant-a");
    private static final TenantDescriptor TENANT_B = TenantDescriptor.tenantWithId("tenant-b");

    private static final GenericCommandResultMessage SUCCESSFUL_COMMAND_RESULT =
            new GenericCommandResultMessage(new MessageType("empty"), "successful");

    private AxonConfiguration configuration;
    private CommandGateway commandGateway;
    private SimpleTenantProvider tenantProvider;

    // Recording projection -- collects processed events per tenant
    static final List<String> processedByTenantA = new CopyOnWriteArrayList<>();
    static final List<String> processedByTenantB = new CopyOnWriteArrayList<>();
    static final List<String> processedByTenantC = new CopyOnWriteArrayList<>();
    static final List<String> renamesByTenantA = new CopyOnWriteArrayList<>();
    static final List<String> renamesByTenantB = new CopyOnWriteArrayList<>();
    static final List<String> renamesByTenantC = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        processedByTenantA.clear();
        processedByTenantB.clear();
        processedByTenantC.clear();
        renamesByTenantA.clear();
        renamesByTenantB.clear();
        renamesByTenantC.clear();

        // given -- two tenants
        tenantProvider = new SimpleTenantProvider();
        tenantProvider.addTenant(TENANT_A);
        tenantProvider.addTenant(TENANT_B);

        // given -- entity module with explicit evolver, criteria resolver, and snapshot after 1 event
        var courseEntity = EventSourcedEntityModule
                .declarative(String.class, CourseState.class)
                .messagingModel((c, b) -> b.entityEvolver(courseEvolver()).build())
                .entityFactory(c -> EventSourcedEntityFactory.fromIdentifier(CourseState::new))
                .criteriaResolver(c -> (courseId, ctx) -> EventCriteria.havingTags("courseId", courseId))
                .snapshotPolicy(c -> SnapshotPolicy.afterEvents(1))
                .build();

        // given -- programmatic command handlers
        var commandHandlingModule = CommandHandlingModule
                .named("CourseCommands")
                .commandHandlers()
                .commandHandler(
                        new QualifiedName(CreateCourse.class),
                        (command, context) -> {
                            // Ensure the command message is on the context for multi-tenant routing
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
                .commandHandler(
                        new QualifiedName(RenameCourse.class),
                        (command, context) -> {
                            // Ensure the command message is on the context for multi-tenant routing
                            var ctx = Message.addToContext(context, command);
                            var payload = command.payloadAs(RenameCourse.class);
                            var stateManager = ctx.component(StateManager.class);
                            var state = stateManager.loadEntity(
                                    CourseState.class, payload.courseId(), ctx
                            ).join();
                            if (state.created) {
                                var eventAppender = EventAppender.forContext(ctx);
                                eventAppender.append(new CourseRenamed(payload.courseId(), payload.newName()));
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
                    String tenantId = event.metadata().get("tenantId");
                    if ("tenant-a".equals(tenantId)) {
                        processedByTenantA.add(payload.courseId());
                    } else if ("tenant-b".equals(tenantId)) {
                        processedByTenantB.add(payload.courseId());
                    } else if ("tenant-c".equals(tenantId)) {
                        processedByTenantC.add(payload.courseId());
                    }
                    return MessageStream.empty();
                }
        );
        courseProjectionComponent.subscribe(
                new QualifiedName(CourseRenamed.class),
                (event, context) -> {
                    var payload = event.payloadAs(CourseRenamed.class);
                    String tenantId = event.metadata().get("tenantId");
                    if ("tenant-a".equals(tenantId)) {
                        renamesByTenantA.add(payload.courseId() + ":" + payload.newName());
                    } else if ("tenant-b".equals(tenantId)) {
                        renamesByTenantB.add(payload.courseId() + ":" + payload.newName());
                    } else if ("tenant-c".equals(tenantId)) {
                        renamesByTenantC.add(payload.courseId() + ":" + payload.newName());
                    }
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

        // Query handling module with annotated component
        var queryHandlingModule = QueryHandlingModule
                .named("CourseQueries")
                .queryHandlers()
                .autodetectedQueryHandlingComponent(config -> new CourseQueryHandler())
                .build();

        // Standard Axon configuration, multi-tenancy applied via SPI enhancer
        var configurer = EventSourcingConfigurer.create();

        configurer.componentRegistry(cr -> {
            cr.registerComponent(TenantProvider.class, config -> tenantProvider);
            cr.registerDecorator(
                    TenantResolverRegistry.class, 0,
                    (config, name, delegate) -> delegate.registerResolver(c -> new MetadataBasedTenantResolver())
            );
            cr.registerComponent(TenantComponentFactory.class, "courseCounter",
                                 config -> new TenantComponentFactory<CourseCounter>() {
                                     @Override
                                     public CourseCounter create(TenantDescriptor tenant) {
                                         return new CourseCounter(tenant.tenantId());
                                     }
                                 });
            // Register a SnapshotStore so the multi-tenancy decorator can wrap it
            cr.registerComponent(SnapshotStore.class, config -> new InMemorySnapshotStore());
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

        configurer.registerQueryHandlingModule(queryHandlingModule);

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
    class CommandRoutingAndEventStorage {

        @Test
        void commandRoutesToCorrectTenantAndProcessorHandlesIt() {
            // when
            sendCommandForTenant("course-1", TENANT_A);
            sendCommandForTenant("course-2", TENANT_B);

            // then
            await().atMost(10, TimeUnit.SECONDS)
                   .untilAsserted(() -> {
                       assertThat(processedByTenantA).contains("course-1");
                       assertThat(processedByTenantB).contains("course-2");
                   });
        }

        @Test
        void multipleCommandsPerTenant() {
            // when
            sendCommandForTenant("course-a1", TENANT_A);
            sendCommandForTenant("course-a2", TENANT_A);
            sendCommandForTenant("course-b1", TENANT_B);

            // then
            await().atMost(10, TimeUnit.SECONDS)
                   .untilAsserted(() -> {
                       assertThat(processedByTenantA).containsExactlyInAnyOrder("course-a1", "course-a2");
                       assertThat(processedByTenantB).containsExactly("course-b1");
                   });
        }
    }

    @Nested
    class TenantIsolation {

        @Test
        void tenantAEventsDoNotAppearInTenantB() {
            // when
            sendCommandForTenant("course-only-a", TENANT_A);

            // then
            await().atMost(10, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(processedByTenantA).contains("course-only-a"));

            assertThat(processedByTenantB).isEmpty();
        }

        @Test
        void tenantBEventsDoNotAppearInTenantA() {
            // when
            sendCommandForTenant("course-only-b", TENANT_B);

            // then
            await().atMost(10, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(processedByTenantB).contains("course-only-b"));

            assertThat(processedByTenantA).isEmpty();
        }
    }

    @Nested
    class EventSourcingIsolation {

        @Test
        void sameEntityIdWorksIndependentlyPerTenant() {
            // when -- same entity ID on both tenants
            sendCommandForTenant("shared-id", TENANT_A);
            sendCommandForTenant("shared-id", TENANT_B);

            // then -- both tenants process the event independently
            await().atMost(10, TimeUnit.SECONDS)
                   .untilAsserted(() -> {
                       assertThat(processedByTenantA).contains("shared-id");
                       assertThat(processedByTenantB).contains("shared-id");
                   });
        }
    }

    @Nested
    class Idempotency {

        @Test
        void duplicateCommandForSameEntityDoesNotProduceDuplicateEvent() {
            // when -- send the same command twice for the same tenant and entity ID;
            //        the handler checks if (!state.created) so the second should produce no event
            sendCommandForTenant("idempotent-1", TENANT_A);
            sendCommandForTenant("idempotent-1", TENANT_A);

            // then -- at least one event is processed; the second command completes without error
            await().atMost(10, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(processedByTenantA).contains("idempotent-1"));

            // then -- the idempotency guard prevented a duplicate event
            await().during(500, TimeUnit.MILLISECONDS)
                   .atMost(2, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(processedByTenantA.stream()
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
            assertThat(multiTenantProcessor.tenantSegments()).hasSize(2);
        }
    }

    @Nested
    class StateManagerSourcing {

        @Test
        void stateManagerLoadsEntityFromCorrectTenantEventStore() {
            // given -- create a course on tenant A
            sendCommandForTenant("sm-course-1", TENANT_A);

            // when -- rename it via StateManager-based handler
            sendCommandForTenant(new RenameCourse("sm-course-1", "Advanced Physics"), TENANT_A);

            // then -- rename event processed on tenant A only
            await().atMost(10, TimeUnit.SECONDS)
                   .untilAsserted(() ->
                           assertThat(renamesByTenantA).contains("sm-course-1:Advanced Physics"));
            assertThat(renamesByTenantB).isEmpty();
        }

        @Test
        void stateManagerIsolatesEntityStateBetweenTenants() {
            // given -- create same course ID on both tenants
            sendCommandForTenant("sm-shared", TENANT_A);
            sendCommandForTenant("sm-shared", TENANT_B);

            // when -- rename on tenant A only
            sendCommandForTenant(new RenameCourse("sm-shared", "Tenant A Name"), TENANT_A);

            // then -- rename processed on tenant A, not B
            await().atMost(10, TimeUnit.SECONDS)
                   .untilAsserted(() ->
                           assertThat(renamesByTenantA).contains("sm-shared:Tenant A Name"));
            assertThat(renamesByTenantB).isEmpty();

            // when -- rename on tenant B
            sendCommandForTenant(new RenameCourse("sm-shared", "Tenant B Name"), TENANT_B);

            // then -- tenant B gets its own rename
            await().atMost(10, TimeUnit.SECONDS)
                   .untilAsserted(() ->
                           assertThat(renamesByTenantB).contains("sm-shared:Tenant B Name"));
        }

        @Test
        void renameFailsSilentlyForNonExistentEntity() {
            // when -- rename a course that was never created on this tenant
            sendCommandForTenant(new RenameCourse("nonexistent", "Some Name"), TENANT_A);

            // then -- no rename event produced (guard: if state.created)
            await().during(500, TimeUnit.MILLISECONDS)
                   .atMost(2, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(renamesByTenantA).isEmpty());
        }
    }

    @Nested
    class TenantScopedComponents {

        @Test
        void tenantComponentFactoryInjectsPerTenantInstance() {
            // when -- query each tenant for its CourseCounter's tenant ID
            String tenantAId = queryTenantIdForTenant(TENANT_A);
            String tenantBId = queryTenantIdForTenant(TENANT_B);

            // then -- each tenant gets its own CourseCounter with the correct tenant ID
            assertThat(tenantAId).isEqualTo("tenant-a");
            assertThat(tenantBId).isEqualTo("tenant-b");
        }

        private String queryTenantIdForTenant(TenantDescriptor tenant) {
            var queryMessage = new GenericQueryMessage(
                    new GenericMessage(
                            new MessageType(GetTenantId.class),
                            new GetTenantId(),
                            Metadata.with("tenantId", tenant.tenantId())
                    )
            );
            QueryBus queryBus = configuration.getComponent(QueryBus.class);
            return (String) queryBus.query(queryMessage, null)
                                    .first()
                                    .asCompletableFuture()
                                    .orTimeout(5, TimeUnit.SECONDS)
                                    .join()
                                    .message()
                                    .payload();
        }
    }

    @Nested
    class QueryRouting {

        @Test
        void queryRoutesToCorrectTenantAndReturnsOnlyThatTenantsData() {
            // given -- events in both tenants
            sendCommandForTenant("qa-1", TENANT_A);
            sendCommandForTenant("qb-1", TENANT_B);
            sendCommandForTenant("qa-2", TENANT_A);

            await().atMost(10, TimeUnit.SECONDS)
                   .untilAsserted(() -> {
                       assertThat(processedByTenantA).containsExactlyInAnyOrder("qa-1", "qa-2");
                       assertThat(processedByTenantB).containsExactly("qb-1");
                   });

            // when -- query tenant A
            List<String> tenantAResult = queryCoursesForTenant(TENANT_A);

            // then
            assertThat(tenantAResult).containsExactlyInAnyOrder("qa-1", "qa-2");
            assertThat(tenantAResult).doesNotContain("qb-1");

            // when -- query tenant B
            List<String> tenantBResult = queryCoursesForTenant(TENANT_B);

            // then
            assertThat(tenantBResult).containsExactly("qb-1");
            assertThat(tenantBResult).doesNotContain("qa-1", "qa-2");
        }
    }

    @Nested
    class Snapshotting {

        @Test
        void snapshotStoreIsMultiTenantWithCorrectSegments() {
            // when
            SnapshotStore snapshotStore = configuration.getComponent(SnapshotStore.class);

            // then
            assertThat(snapshotStore).isInstanceOf(TenantRoutingSnapshotStore.class);
            TenantRoutingSnapshotStore multiTenantStore = (TenantRoutingSnapshotStore) snapshotStore;
            assertThat(multiTenantStore.tenantSegments()).containsKey(TENANT_A);
            assertThat(multiTenantStore.tenantSegments()).containsKey(TENANT_B);
            assertThat(multiTenantStore.tenantSegments()).hasSize(2);
        }

        @Test
        void snapshotCreatedAfterEventSourcingOnCorrectTenant() {
            // given -- snapshot policy is afterEvents(1), triggers when > 1 events applied
            //          create + rename = 2 events, which triggers snapshot on next load
            sendCommandForTenant("snap-course-1", TENANT_A);
            sendCommandForTenant(new RenameCourse("snap-course-1", "Physics"), TENANT_A);

            // when -- load the entity again (2 events replayed → triggers snapshot creation)
            sendCommandForTenant("snap-course-1", TENANT_A);

            // then -- verify snapshot exists in tenant A's store
            TenantRoutingSnapshotStore multiTenantStore =
                    (TenantRoutingSnapshotStore) configuration.getComponent(SnapshotStore.class);
            SnapshotStore tenantAStore = multiTenantStore.tenantSegments().get(TENANT_A);

            var entityName = new QualifiedName(CourseState.class);
            Snapshot snapshot = tenantAStore.load(entityName, "snap-course-1", null)
                                            .orTimeout(5, TimeUnit.SECONDS)
                                            .join();
            assertThat(snapshot).isNotNull();
            assertThat(snapshot.payload()).isInstanceOf(CourseState.class);
            assertThat(((CourseState) snapshot.payload()).created).isTrue();
        }

        @Test
        void snapshotIsolatedBetweenTenants() {
            // given -- create + rename on tenant A to exceed afterEvents(1) threshold
            sendCommandForTenant("snap-isolated", TENANT_A);
            sendCommandForTenant(new RenameCourse("snap-isolated", "Chemistry"), TENANT_A);

            // when -- load again to trigger snapshot
            sendCommandForTenant("snap-isolated", TENANT_A);

            // then -- snapshot exists in tenant A but NOT in tenant B
            TenantRoutingSnapshotStore multiTenantStore =
                    (TenantRoutingSnapshotStore) configuration.getComponent(SnapshotStore.class);
            var entityName = new QualifiedName(CourseState.class);

            Snapshot fromA = multiTenantStore.tenantSegments().get(TENANT_A)
                    .load(entityName, "snap-isolated", null)
                    .orTimeout(5, TimeUnit.SECONDS).join();
            Snapshot fromB = multiTenantStore.tenantSegments().get(TENANT_B)
                    .load(entityName, "snap-isolated", null)
                    .orTimeout(5, TimeUnit.SECONDS).join();

            assertThat(fromA).isNotNull();
            assertThat(fromB).isNull();
        }
    }

    @Nested
    class DynamicTenantRegistration {

        private static final TenantDescriptor TENANT_C = TenantDescriptor.tenantWithId("tenant-c");

        @Test
        void dynamicallyAddedTenantCanProcessCommands() {
            // given -- system started with only tenant-a and tenant-b
            assertThat(tenantProvider.hasTenant(TENANT_C)).isFalse();

            // when -- add tenant-c at runtime
            tenantProvider.addTenant(TENANT_C);

            // then -- tenant-c is recognized
            assertThat(tenantProvider.hasTenant(TENANT_C)).isTrue();

            // when -- send a command for tenant-c
            sendCommandForTenant("dynamic-course-1", TENANT_C);

            // then -- event is processed on tenant-c
            await().atMost(10, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(processedByTenantC).contains("dynamic-course-1"));

            // then -- existing tenants are unaffected
            assertThat(processedByTenantA).isEmpty();
            assertThat(processedByTenantB).isEmpty();
        }

        @Test
        void dynamicallyAddedTenantGetsItsOwnEventProcessorSegment() {
            // when
            tenantProvider.addTenant(TENANT_C);

            // then -- the multi-tenant event processor now has 3 segments
            Map<String, EventProcessor> processors = configuration.getComponents(EventProcessor.class);
            MultiTenantEventProcessor multiTenantProcessor =
                    (MultiTenantEventProcessor) processors.get("CourseProjection");

            assertThat(multiTenantProcessor.tenantSegments()).hasSize(3);
            assertThat(multiTenantProcessor.tenantSegments()).containsKey(TENANT_C);
        }

        @Test
        void dynamicallyAddedTenantSupportsFullEventSourcingCycle() {
            // given -- add tenant-c and create an entity
            tenantProvider.addTenant(TENANT_C);
            sendCommandForTenant("sourced-c-1", TENANT_C);

            await().atMost(10, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertThat(processedByTenantC).contains("sourced-c-1"));

            // when -- rename the entity (loads state via event sourcing, then appends)
            sendCommandForTenant(new RenameCourse("sourced-c-1", "Dynamic Course"), TENANT_C);

            // then -- rename event processed on tenant-c
            await().atMost(10, TimeUnit.SECONDS)
                   .untilAsserted(() ->
                           assertThat(renamesByTenantC).contains("sourced-c-1:Dynamic Course"));

            // then -- other tenants unaffected
            assertThat(renamesByTenantA).isEmpty();
            assertThat(renamesByTenantB).isEmpty();
        }
    }

    // --- Helpers ---

    private void sendCommandForTenant(String courseId, TenantDescriptor tenant) {
        sendCommandForTenant(new CreateCourse(courseId), tenant);
    }

    private void sendCommandForTenant(Object command, TenantDescriptor tenant) {
        commandGateway.send(command, Metadata.with("tenantId", tenant.tenantId()), null)
                      .getResultMessage()
                      .orTimeout(10, TimeUnit.SECONDS)
                      .join();
    }

    private List<String> queryCoursesForTenant(TenantDescriptor tenant) {
        var queryMessage = new GenericQueryMessage(
                new GenericMessage(
                        new MessageType(GetCourses.class),
                        new GetCourses(),
                        Metadata.with("tenantId", tenant.tenantId())
                )
        );
        QueryBus queryBus = configuration.getComponent(QueryBus.class);
        return queryBus.query(queryMessage, null)
                       .<List<String>>reduce(new ArrayList<>(), (list, entry) -> {
                           list.add((String) entry.message().payload());
                           return list;
                       })
                       .orTimeout(10, TimeUnit.SECONDS)
                       .join();
    }

    // --- Domain ---

    record CreateCourse(@TargetEntityId String courseId) {
    }

    record RenameCourse(@TargetEntityId String courseId, String newName) {
    }

    record CourseCreated(@EventTag String courseId) {
    }

    record CourseRenamed(@EventTag String courseId, String newName) {
    }

    record GetCourses() {
    }

    record GetTenantId() {
    }

    /**
     * A simple tenant-scoped component -- each tenant gets its own instance with the tenant ID baked in.
     */
    static class CourseCounter {

        final String tenantId;

        CourseCounter(String tenantId) {
            this.tenantId = tenantId;
        }
    }

    /**
     * Mutable course state for event sourcing.
     */
    static class CourseState {

        boolean created;
        String name;

        CourseState(String id) {
            this.created = false;
        }
    }

    private static EntityEvolver<CourseState> courseEvolver() {
        return (entity, event, context) -> {
            if (event.type().qualifiedName().equals(new QualifiedName(CourseCreated.class))) {
                entity.created = true;
            } else if (event.type().qualifiedName().equals(new QualifiedName(CourseRenamed.class))) {
                var payload = event.payloadAs(CourseRenamed.class);
                entity.name = payload.newName();
            }
            return entity;
        };
    }

    /**
     * Query handler component for course queries and tenant-scoped component testing.
     */
    static class CourseQueryHandler {

        @QueryHandler
        List<String> handle(GetCourses query,
                            @org.axonframework.messaging.core.annotation.MetadataValue("tenantId") String tenantId) {
            if ("tenant-a".equals(tenantId)) {
                return List.copyOf(processedByTenantA);
            } else if ("tenant-b".equals(tenantId)) {
                return List.copyOf(processedByTenantB);
            } else if ("tenant-c".equals(tenantId)) {
                return List.copyOf(processedByTenantC);
            }
            return List.of();
        }

        @QueryHandler
        String handle(GetTenantId query, CourseCounter counter) {
            return counter.tenantId;
        }
    }
}