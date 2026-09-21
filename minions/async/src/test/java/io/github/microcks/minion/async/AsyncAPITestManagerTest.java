/*
 * Copyright The Microcks Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.microcks.minion.async;

import io.github.microcks.domain.Resource;
import io.github.microcks.domain.Service;
import io.github.microcks.domain.ServiceView;
import io.github.microcks.domain.TestCaseResult;
import io.github.microcks.domain.TestReturn;
import io.github.microcks.minion.async.client.KeycloakConfig;
import io.github.microcks.minion.async.client.MicrocksAPIConnector;
import io.github.microcks.minion.async.client.dto.TestCaseReturnDTO;
import io.github.microcks.minion.async.consumer.ConsumedMessage;
import io.github.microcks.minion.async.consumer.MessageConsumptionTask;
import io.github.microcks.util.asyncapi.AsyncAPISchemaUtil;
import io.github.microcks.util.asyncapi.AsyncAPISchemaValidator;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

public class AsyncAPITestManagerTest {

   private static final String ASYNC_API_2_SCHEMA_TEXT1 = """
         asyncapi: '2.0.0'
         info:
           title: Example
           version: '1.0.0'
           description: Example YAML
         channels:
           sendMessage:
             description: Example YAML
             publish:
               summary: Example YAML
               operationId: sendMessage
               message:
                 $ref: '#/components/messages/send'
         components:
           messages:
             send:
               summary: Example message
               contentType: text/plain
         """;

   @Test
   void testGetExpectedContentType() {
      JsonNode specificationNode = null;
      try {
         specificationNode = AsyncAPISchemaValidator.getJsonNodeForSchema(ASYNC_API_2_SCHEMA_TEXT1);
      } catch (IOException e) {
         Assertions.fail("Exception should not be thrown");
      }
      String messagePathPointer = AsyncAPISchemaUtil.findMessagePathPointer(specificationNode, "SEND sendMessage");
      Assertions.assertNotNull(messagePathPointer);
      Assertions.assertTrue(messagePathPointer.contains("/channels/sendMessage/publish/message"));
      String expectedContentType = getExpectedContentType(specificationNode, messagePathPointer);
      Assertions.assertTrue(expectedContentType.contains("text/plain"));
   }

   /** Retrieve the expected content type for an AsyncAPI message. */
   private String getExpectedContentType(JsonNode specificationNode, String messagePathPointer) {
      // Retrieve default content type, defaulting to application/json.
      String defaultContentType = specificationNode.path("defaultContentType").asText("application/json");
      // Get message real content type if defined.
      String contentType = defaultContentType;
      JsonNode messageNode = specificationNode.at(messagePathPointer);
      // messageNode will be an array of messages
      if (messageNode.isArray() && messageNode.size() > 0) {
         messageNode = messageNode.get(0);
      }
      // If it's a $ref, then navigate to it.
      while (messageNode.has("$ref")) {
         // $ref: '#/components/messages/lightMeasured'
         String ref = messageNode.path("$ref").asText();
         messageNode = specificationNode.at(ref.substring(1));
      }
      if (messageNode.has("contentType")) {
         contentType = messageNode.path("contentType").asText();
      }
      return contentType;
   }

   @Test
   void testRunReportsResultWhenNoConsumptionTaskMatches() {
      RecordingMicrocksAPIConnector connector = new RecordingMicrocksAPIConnector();
      AsyncAPITestManager manager = new AsyncAPITestManager(connector, new SchemaRegistry(connector));

      AsyncTestSpecification specification = new AsyncTestSpecification();
      specification.setTestResultId("test-result-id");
      specification.setOperationName("SEND sendMessage");
      specification.setEndpointUrl("unsupported://nowhere");
      specification.setTimeoutMS(500L);

      // Run synchronously so that the test stays deterministic.
      manager.new AsyncAPITestThread(specification).run();

      Assertions.assertNotNull(connector.reported, "A test case result must always be reported");
      Assertions.assertEquals(1, connector.reported.getTestReturns().size());
      Assertions.assertEquals(TestReturn.FAILURE_CODE, connector.reported.getTestReturns().get(0).getCode());
   }

   @Test
   void testRunReportsResultWhenValidationThrows() {
      RecordingMicrocksAPIConnector connector = new RecordingMicrocksAPIConnector();
      AsyncAPITestManager manager = new AsyncAPITestManager(connector, new SchemaRegistry(connector));

      AsyncTestSpecification specification = new AsyncTestSpecification();
      specification.setTestResultId("test-result-id");
      // An operation name without a space makes findMessagePathPointer raise an ArrayIndexOutOfBoundsException:
      // an unchecked exception that used to kill the test thread, leaving the TestResult in progress forever.
      specification.setOperationName("sendMessage");
      specification.setAsyncAPISpec(ASYNC_API_2_SCHEMA_TEXT1);
      specification.setEndpointUrl("fake://nowhere");
      specification.setTimeoutMS(500L);

      ConsumedMessage message = new ConsumedMessage();
      message.setReceivedAt(System.currentTimeMillis());
      message.setPayload("hello".getBytes(StandardCharsets.UTF_8));

      // Consume one message without any broker, so that the run reaches the validation stage.
      AsyncAPITestManager.AsyncAPITestThread thread = manager.new AsyncAPITestThread(specification) {
         @Override
         MessageConsumptionTask buildMessageConsumptionTask(AsyncTestSpecification testSpecification) {
            return new MessageConsumptionTask() {
               @Override
               public List<ConsumedMessage> call() {
                  return List.of(message);
               }

               @Override
               public void close() {
                  // Nothing to close.
               }
            };
         }
      };

      // Run synchronously so that the test stays deterministic. No exception must escape.
      Assertions.assertDoesNotThrow(thread::run);

      Assertions.assertNotNull(connector.reported, "A test case result must always be reported");
      Assertions.assertFalse(connector.reported.getTestReturns().isEmpty());
      Assertions.assertEquals(TestReturn.FAILURE_CODE, connector.reported.getTestReturns().get(0).getCode());
      Assertions.assertTrue(connector.reported.getTestReturns().get(0).getMessage().contains("unexpected failure"));
   }

   /** A MicrocksAPIConnector recording the reported test case result. */
   private static class RecordingMicrocksAPIConnector implements MicrocksAPIConnector {

      private TestCaseReturnDTO reported;

      @Override
      public KeycloakConfig getKeycloakConfig() {
         return null;
      }

      @Override
      public List<Service> listServices(String authorization, int page, int size) {
         return Collections.emptyList();
      }

      @Override
      public ServiceView getService(String authorization, String serviceId, boolean messages) {
         return null;
      }

      @Override
      public List<Resource> getResources(String serviceId) {
         return Collections.emptyList();
      }

      @Override
      public TestCaseResult reportTestCaseResult(String testResultId, TestCaseReturnDTO testCaseReturn) {
         this.reported = testCaseReturn;
         return null;
      }
   }
}
