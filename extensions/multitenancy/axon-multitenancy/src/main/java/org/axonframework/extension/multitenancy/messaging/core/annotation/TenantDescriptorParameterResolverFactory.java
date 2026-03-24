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
package org.axonframework.extension.multitenancy.messaging.core.annotation;

import org.axonframework.extension.multitenancy.common.TargetTenantResolver;
import org.axonframework.extension.multitenancy.common.TenantDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Executable;
import java.util.Objects;
import java.lang.reflect.Parameter;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link ParameterResolverFactory} that resolves {@link TenantDescriptor} parameters in message handlers.
 * <p>
 * When a handler method declares a {@link TenantDescriptor} parameter, this factory provides
 * the descriptor for the tenant that the current message belongs to, as determined by the
 * configured {@link TargetTenantResolver}.
 * <p>
 * This gives handlers direct access to the tenant's identity and properties without
 * needing to extract the tenant ID from message metadata manually.
 * <p>
 * Example usage:
 * <pre>{@code
 * @EventHandler
 * void on(OrderPlacedEvent event, TenantDescriptor tenant) {
 *     log.info("Processing order for tenant {} in region {}",
 *              tenant.tenantId(), tenant.properties().get("region"));
 * }
 * }</pre>
 *
 * @author Theo Emanuelsson
 * @since 5.2.0
 * @see TenantDescriptor
 * @see TargetTenantResolver
 */
public class TenantDescriptorParameterResolverFactory implements ParameterResolverFactory {

    private final TargetTenantResolver<Message> tenantResolver;

    /**
     * Creates a new {@link TenantDescriptorParameterResolverFactory}.
     *
     * @param tenantResolver the resolver used to determine which tenant a message belongs to
     */
    public TenantDescriptorParameterResolverFactory(TargetTenantResolver<Message> tenantResolver) {
        this.tenantResolver = Objects.requireNonNull(tenantResolver, "TargetTenantResolver must not be null");
    }

    @Nullable
    @Override
    public ParameterResolver<?> createInstance(Executable executable,
                                               Parameter[] parameters,
                                               int parameterIndex) {
        if (parameters[parameterIndex].getType() == TenantDescriptor.class) {
            return new TenantDescriptorResolver();
        }
        return null;
    }

    private class TenantDescriptorResolver implements ParameterResolver<TenantDescriptor> {

        @Override
        public CompletableFuture<TenantDescriptor> resolveParameterValue(ProcessingContext context) {
            Message message = Message.fromContext(context);
            if (message == null) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("No message found in ProcessingContext"));
            }
            try {
                TenantDescriptor tenant = tenantResolver.resolveTenant(message, Collections.emptyList());
                return CompletableFuture.completedFuture(tenant);
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override
        public boolean matches(ProcessingContext context) {
            Message message = Message.fromContext(context);
            if (message == null) {
                return false;
            }
            try {
                return tenantResolver.resolveTenant(message, Collections.emptyList()) != null;
            } catch (Exception e) {
                return false;
            }
        }
    }
}
