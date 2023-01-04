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

import java.io.Serializable;

/**
 * The deserialization schema describes how to turn the PubSub message into data types that
 * are processed by the system.
 *
 * @param <T> The type created by the keyed deserialization schema.
 */
public interface PubSubIngressDeserializer<T> extends Serializable {

  /**
   * Deserializes the PubSub record.
   *
   * @param message PubSub message to be deserialized.
   * @return The deserialized message as an object (null if the message cannot be deserialized).
   */
  T deserialize(PubsubMessage message);
}
