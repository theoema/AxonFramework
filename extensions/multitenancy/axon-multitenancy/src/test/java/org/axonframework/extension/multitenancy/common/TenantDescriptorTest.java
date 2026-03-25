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

package org.axonframework.extension.multitenancy.common;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link TenantDescriptor}.
 *
 * @author Steven van Beelen
 */
class TenantDescriptorTest {

    private static final String TENANT_ID_ONE = "me";
    private static final String TENANT_ID_TWO = "you";

    private HashMap<String, String> testPropertiesOne;
    private HashMap<String, String> testPropertiesTwo;

    private TenantDescriptor testSubjectOne;
    private TenantDescriptor testSubjectTwo;
    private TenantDescriptor testSubjectThree;
    private TenantDescriptor testSubjectFour;
    private TenantDescriptor testSubjectFive;

    @BeforeEach
    void setUp() {
        testPropertiesOne = new HashMap<>();
        testPropertiesOne.put("key", "value");
        testPropertiesOne.put("key1", "value2");
        testPropertiesTwo = new HashMap<>();
        testPropertiesTwo.put("value", "key");
        testPropertiesTwo.put("value2", "key1");

        testSubjectOne = TenantDescriptor.tenantWithId(TENANT_ID_ONE);
        testSubjectTwo = TenantDescriptor.tenantWithId(TENANT_ID_TWO);
        testSubjectThree = new TenantDescriptor(TENANT_ID_ONE, testPropertiesOne);
        testSubjectFour = new TenantDescriptor(TENANT_ID_TWO, testPropertiesTwo);
        testSubjectFive = new TenantDescriptor(TENANT_ID_ONE, testPropertiesTwo);
    }

    @Nested
    class Equals {

        @Test
        void equalsOnlyValidatesTenantId() {
            // then - validate test subject one, only matching on tenant id
            assertThat(testSubjectOne).isNotEqualTo(testSubjectTwo);
            assertThat(testSubjectOne).isEqualTo(testSubjectThree);
            assertThat(testSubjectOne).isNotEqualTo(testSubjectFour);
            assertThat(testSubjectOne).isEqualTo(testSubjectFive);

            // then - validate test subject two, only matching on tenant id
            assertThat(testSubjectTwo).isNotEqualTo(testSubjectThree);
            assertThat(testSubjectTwo).isEqualTo(testSubjectFour);
            assertThat(testSubjectTwo).isNotEqualTo(testSubjectFive);

            // then - validate test subject three, only matching on tenant id
            assertThat(testSubjectThree).isNotEqualTo(testSubjectFour);
            assertThat(testSubjectThree).isEqualTo(testSubjectFive);

            // then - validate test subject four, only matching on tenant id
            assertThat(testSubjectFour).isNotEqualTo(testSubjectFive);
        }
    }

    @Nested
    class HashCode {

        @Test
        void hashOnlyHashesTenantId() {
            // then - validate test subject one, only matching on tenant id
            assertThat(testSubjectOne.hashCode()).isNotEqualTo(testSubjectTwo.hashCode());
            assertThat(testSubjectOne.hashCode()).isEqualTo(testSubjectThree.hashCode());
            assertThat(testSubjectOne.hashCode()).isNotEqualTo(testSubjectFour.hashCode());
            assertThat(testSubjectOne.hashCode()).isEqualTo(testSubjectFive.hashCode());

            // then - validate test subject two, only matching on tenant id
            assertThat(testSubjectTwo.hashCode()).isNotEqualTo(testSubjectThree.hashCode());
            assertThat(testSubjectTwo.hashCode()).isEqualTo(testSubjectFour.hashCode());
            assertThat(testSubjectTwo.hashCode()).isNotEqualTo(testSubjectFive.hashCode());

            // then - validate test subject three, only matching on tenant id
            assertThat(testSubjectThree.hashCode()).isNotEqualTo(testSubjectFour.hashCode());
            assertThat(testSubjectThree.hashCode()).isEqualTo(testSubjectFive.hashCode());

            // then - validate test subject four, only matching on tenant id
            assertThat(testSubjectFour.hashCode()).isNotEqualTo(testSubjectFive.hashCode());
        }
    }
}
