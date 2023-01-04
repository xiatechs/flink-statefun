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

import com.google.auth.Credentials;
import org.apache.flink.statefun.sdk.annotations.ForRuntime;
import org.apache.flink.statefun.sdk.core.OptionalProperty;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.EmulatorCredentials;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;
import java.util.Properties;

import static com.google.cloud.pubsub.v1.SubscriptionAdminSettings.defaultCredentialsProviderBuilder;

/**
 * A builder for creating an {@link IngressSpec} for consuming data from GCP PubSub.
 *
 * @param <T> The type consumed from PubSub.
 */
public final class PubSubIngressBuilder<T> {

  private final IngressIdentifier<T> id;
  private String projectName;
  private String hostAndPort;
  private String subscriptionName;
  private Credentials credentials;
  private final Properties properties = new Properties();
  private OptionalProperty<PubSubIngressDeserializer<T>> deserializer =
      OptionalProperty.withoutDefault();

  private PubSubIngressBuilder(IngressIdentifier<T> id) {
    this.id = Objects.requireNonNull(id);
  }

  /**
   * @param id A unique ingress identifier.
   * @param <T> The type consumed from PubSub.
   * @return A new {@link PubSubIngressBuilder}.
   */
  public static <T> PubSubIngressBuilder<T> forIdentifier(IngressIdentifier<T> id) {
    return new PubSubIngressBuilder<>(id);
  }

  /** @param projectName the consumer group id to use. */
  public PubSubIngressBuilder<T> withProjectName(String projectName) {
    this.projectName = projectName;
    return this;
  }

  /** @param subscriptionName Comma separated addresses of the brokers. */
  public PubSubIngressBuilder<T> withSubscriptionName(String subscriptionName) {
    this.subscriptionName = subscriptionName;
    return this;
  }

  /** @param hostAndPort The name of the topic that should be consumed. */
  public PubSubIngressBuilder<T> withHostAndPortForEmulator(String hostAndPort) {
    this.hostAndPort = hostAndPort;
    return this;
  }

  /** A configuration property for the PubSubConsumer. */
  public PubSubIngressBuilder<T> withProperties(Properties properties) {
    this.properties.putAll(properties);
    return this;
  }

  public PubSubIngressBuilder<T> withCredentials(Credentials credentials) {
    this.credentials = credentials;
    return this;
  }

  /** A configuration property for the PubSubProducer. */
  public PubSubIngressBuilder<T> withProperty(String name, String value) {
    Objects.requireNonNull(name);
    Objects.requireNonNull(value);
    this.properties.setProperty(name, value);
    return this;
  }

  /**
   * @param deserializerClass The deserializer used to convert between PubSubs byte messages and
   *     java objects.
   */
  public PubSubIngressBuilder<T> withDeserializer(
      Class<? extends PubSubIngressDeserializer<T>> deserializerClass) {
    Objects.requireNonNull(deserializerClass);
    this.deserializer.set(instantiateDeserializer(deserializerClass));
    return this;
  }

  /** @return A new {@link PubSubIngressSpec}. */
  public PubSubIngressSpec<T> build() throws IOException {
    if (credentials == null) {
      if (hostAndPort == null) {
        // No hostAndPort is the normal scenario, so we use the default credentials.
        credentials = defaultCredentialsProviderBuilder().build().getCredentials();
      } else {
        // With hostAndPort the PubSub emulator is used, so we do not have credentials.
        credentials = EmulatorCredentials.getInstance();
      }
    }

    return new PubSubIngressSpec<>(
        id, properties, projectName,subscriptionName, deserializer.get(), credentials);
  }

//  private Properties resolvePubSubProperties() {
//    Properties resultProps = new Properties();
//    resultProps.putAll(properties);
//
//    // for all configuration passed using named methods, overwrite corresponding properties
//    kafkaAddress.overwritePropertiesIfPresent(resultProps, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
//    autoResetPosition.overwritePropertiesIfPresent(
//        resultProps, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
//    consumerGroupId.overwritePropertiesIfPresent(resultProps, ConsumerConfig.GROUP_ID_CONFIG);
//
//    return resultProps;
//  }

  private static <T extends PubSubIngressDeserializer<?>> T instantiateDeserializer(
      Class<T> deserializerClass) {
    try {
      Constructor<T> defaultConstructor = deserializerClass.getDeclaredConstructor();
      defaultConstructor.setAccessible(true);
      return defaultConstructor.newInstance();
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
          "Unable to create an instance of deserializer "
              + deserializerClass.getName()
              + "; has no default constructor",
          e);
    } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
      throw new IllegalStateException(
          "Unable to create an instance of deserializer " + deserializerClass.getName(), e);
    }
  }

  // ========================================================================================
  //  Methods for runtime usage
  // ========================================================================================

  @ForRuntime
  PubSubIngressBuilder<T> withDeserializer(PubSubIngressDeserializer<T> deserializer) {
    this.deserializer.set(deserializer);
    return this;
  }
}
