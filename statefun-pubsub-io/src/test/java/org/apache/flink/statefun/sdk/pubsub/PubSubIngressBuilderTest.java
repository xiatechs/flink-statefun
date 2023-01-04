/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.statefun.sdk.pubsub;

import com.google.pubsub.v1.PubsubMessage;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class PubSubIngressBuilderTest {

    private static final IngressIdentifier<String> ID =
            new IngressIdentifier<>(String.class, "namespace", "name");

    private static final String PROJECT_NAME = "test-project";
    private static final String SUBSCRIPTION_NAME = "test-subscription";

    @Test
    public void exampleUsage() throws IOException {
        // TODO: Test credentials
        final PubSubIngressSpec<String> pubSubIngressSpec =
                PubSubIngressBuilder.forIdentifier(ID)
                        .withDeserializer(TestDeserializer.class)
                        .withProjectName(PROJECT_NAME)
                        .withSubscriptionName(SUBSCRIPTION_NAME)
                        .build();

        assertThat(pubSubIngressSpec.id(), is(ID));
        assertThat(pubSubIngressSpec.projectName(), is(PROJECT_NAME));
        assertThat(pubSubIngressSpec.subscriptionName(), is(SUBSCRIPTION_NAME));
        assertThat(pubSubIngressSpec.deserializer(), instanceOf(TestDeserializer.class));
        assertTrue(pubSubIngressSpec.properties().isEmpty());
    }

    private static final class TestDeserializer implements PubSubIngressDeserializer<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public String deserialize(PubsubMessage message) {
            return null;
        }
    }
}
