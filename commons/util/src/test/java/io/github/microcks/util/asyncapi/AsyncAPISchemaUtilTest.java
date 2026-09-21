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
package io.github.microcks.util.asyncapi;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * This is a test case for AsyncAPISchemaUtil utility.
 * @author laurent
 */
class AsyncAPISchemaUtilTest {

   @Test
   void testIsAvroSchemaFormat() {
      // Both the plain and the +json flavors, with or without a version parameter, denote Avro.
      assertTrue(AsyncAPISchemaUtil.isAvroSchemaFormat("application/vnd.apache.avro"));
      assertTrue(AsyncAPISchemaUtil.isAvroSchemaFormat("application/vnd.apache.avro;version=1.9.0"));
      assertTrue(AsyncAPISchemaUtil.isAvroSchemaFormat("application/vnd.apache.avro+json;version=1.9.0"));
      assertTrue(AsyncAPISchemaUtil.isAvroSchemaFormat("application/vnd.apache.avro;version=1.11.0"));

      assertFalse(AsyncAPISchemaUtil.isAvroSchemaFormat(null));
      assertFalse(AsyncAPISchemaUtil.isAvroSchemaFormat("application/schema+json;version=draft-07"));
      assertFalse(AsyncAPISchemaUtil.isAvroSchemaFormat("application/vnd.aai.asyncapi;version=3.0.0"));
   }

   @Test
   void testUnwrapAvroMultiFormatSchemaOnInlineSchema() throws IOException {
      // An AsyncAPI v3 Multi Format Schema Object: the inner schema must be extracted.
      JsonNode payload = AsyncAPISchemaValidator.getJsonNode("""
            {
               "schemaFormat": "application/vnd.apache.avro;version=1.9.0",
               "schema": {
                  "type": "record", "name": "User",
                  "fields": [{"name": "fullName", "type": "string"}]
               }
            }
            """);

      JsonNode unwrapped = AsyncAPISchemaUtil.unwrapAvroMultiFormatSchema(payload, null);

      assertEquals("record", unwrapped.path("type").asText());
      assertEquals("User", unwrapped.path("name").asText());
   }

   @Test
   void testUnwrapAvroMultiFormatSchemaWithHoistedSchemaFormat() throws IOException {
      // Some v3 documents keep declaring schemaFormat on the Message Object as AsyncAPI v2 was doing.
      JsonNode payload = AsyncAPISchemaValidator.getJsonNode("""
            {"schema": {"type": "record", "name": "User", "fields": []}}
            """);

      JsonNode unwrapped = AsyncAPISchemaUtil.unwrapAvroMultiFormatSchema(payload,
            "application/vnd.apache.avro+json;version=1.9.0");

      assertEquals("User", unwrapped.path("name").asText());
   }

   @Test
   void testUnwrapAvroMultiFormatSchemaLeavesAsyncAPIV2PayloadUntouched() throws IOException {
      // An AsyncAPI v2 payload is directly the Avro schema and must be returned as is.
      JsonNode payload = AsyncAPISchemaValidator.getJsonNode("""
            {"type": "record", "name": "User", "fields": [{"name": "fullName", "type": "string"}]}
            """);

      assertSame(payload,
            AsyncAPISchemaUtil.unwrapAvroMultiFormatSchema(payload, "application/vnd.apache.avro;version=1.9.0"));
   }

   @Test
   void testUnwrapAvroMultiFormatSchemaLeavesAvroSchemaMetadataUntouched() throws IOException {
      // An Avro schema is allowed to carry unknown attributes as metadata - even one named 'schema'. The presence
      // of a 'type' attribute is what tells such a payload apart from a Multi Format Schema Object.
      JsonNode payload = AsyncAPISchemaValidator.getJsonNode("""
            {
               "type": "record", "name": "User", "schema": "some-metadata",
               "fields": [{"name": "fullName", "type": "string"}]
            }
            """);

      assertSame(payload,
            AsyncAPISchemaUtil.unwrapAvroMultiFormatSchema(payload, "application/vnd.apache.avro;version=1.9.0"));
   }

   @Test
   void testUnwrapAvroMultiFormatSchemaLeavesNonAvroWrapperUntouched() throws IOException {
      // A JSON Schema Multi Format Schema Object must not be unwrapped by the Avro path.
      JsonNode payload = AsyncAPISchemaValidator.getJsonNode("""
            {
               "schemaFormat": "application/schema+json;version=draft-07",
               "schema": {"type": "object", "properties": {"fullName": {"type": "string"}}}
            }
            """);

      assertSame(payload,
            AsyncAPISchemaUtil.unwrapAvroMultiFormatSchema(payload, "application/vnd.apache.avro;version=1.9.0"));
   }

   @Test
   void testFindMessagePathPointer() throws IOException {
      JsonNode v2Spec = AsyncAPISchemaValidator.getJsonNode("""
            {"asyncapi": "2.6.0", "channels": {}}
            """);
      assertEquals("/channels/user~1signedup/publish/message",
            AsyncAPISchemaUtil.findMessagePathPointer(v2Spec, "SEND user/signedup"));
      assertEquals("/channels/user~1signedup/subscribe/message",
            AsyncAPISchemaUtil.findMessagePathPointer(v2Spec, "SUBSCRIBE user/signedup"));

      JsonNode v3Spec = AsyncAPISchemaValidator.getJsonNode("""
            {"asyncapi": "3.0.0", "operations": {}}
            """);
      assertEquals("/operations/publishUserSignedUps/messages",
            AsyncAPISchemaUtil.findMessagePathPointer(v3Spec, "SEND publishUserSignedUps"));
   }
}
