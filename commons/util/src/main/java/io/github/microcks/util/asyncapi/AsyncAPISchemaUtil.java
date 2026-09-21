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

import io.github.microcks.util.AvroUtil;
import io.github.microcks.util.SchemaMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for extracting information from AsyncAPI schema. Supported version of AsyncAPI schema are
 * https://www.asyncapi.com/docs/reference/specification/v3.0.0 and
 * https://www.asyncapi.com/docs/reference/specification/v2.x.
 * @author laurent
 */
public class AsyncAPISchemaUtil {

   /** A simple logger for diagnostic messages. */
   private static final Logger log = LoggerFactory.getLogger(AsyncAPISchemaUtil.class);

   public static final String ASYNC_SCHEMA_PAYLOAD_ELEMENT = "payload";

   /**
    * Prefix of the {@code schemaFormat} media types denoting an Apache Avro schema. Real-world documents use both
    * {@code application/vnd.apache.avro;version=1.9.0} and {@code application/vnd.apache.avro+json;version=1.9.0} so
    * detection is always done on a prefix basis.
    */
   public static final String AVRO_SCHEMA_FORMAT_PREFIX = "application/vnd.apache.avro";

   private static final String ONE_OF_STRUCT = "oneOf";
   private static final String REF_ELEMENT = "$ref";
   private static final String SCHEMA_ELEMENT = "schema";
   private static final String SCHEMA_FORMAT_ELEMENT = "schemaFormat";
   private static final String TYPE_ELEMENT = "type";


   /** Private constructor to hide the implicit one. */
   private AsyncAPISchemaUtil() {
   }

   /** Define the JSON pointer expression to access the operation messages. */
   public static String findMessagePathPointer(JsonNode specificationNode, String operationName) {
      String messagePathPointer = "";
      String[] operationElements = operationName.split(" ");

      String asyncApi = specificationNode.path("asyncapi").asText("2");
      if (asyncApi.startsWith("3")) {
         // Assume we have an AsyncAPI v3 document.
         String operationNamePtr = "/operations/" + operationElements[1].replace("/", "~1");
         messagePathPointer = operationNamePtr + "/messages";
      } else {
         // Assume we have an AsyncAPI v2 document.
         String operationNamePtr = "/channels/" + operationElements[1].replace("/", "~1");
         if ("SUBSCRIBE".equals(operationElements[0])) {
            operationNamePtr += "/subscribe";
         } else {
            operationNamePtr += "/publish";
         }
         messagePathPointer = operationNamePtr + "/message";
      }
      return messagePathPointer;
   }

   /**
    * Retrieve the Avro schema corresponding to a message using its JSON pointer in Spec. Complete the {@code schemaMap}
    * if provided. Raise an {@link AsyncAPISchemaException} with message if problem while navigating the spec.
    * @throws AsyncAPISchemaException if a problem occurs while navigating the spec
    */
   public static Schema retrieveMessageAvroSchema(JsonNode specificationNode, String messagePathPointer,
         SchemaMap schemaMap) throws AsyncAPISchemaException {
      // Extract Json node for message pointer.
      JsonNode messageNode = specificationNode.at(messagePathPointer);
      if (messageNode == null || messageNode.isMissingNode()) {
         log.debug("messagePathPointer {} is not a valid JSON Pointer", messagePathPointer);
         throw new AsyncAPISchemaException(
               "messagePathPointer does not represent a valid JSON Pointer in AsyncAPI specification");
      }
      // Message node can be just a reference.
      messageNode = followRefIfAny(messageNode, specificationNode);

      if (messageNode.isArray()) {
         // In the case of AsyncAPI v3, we always got messages even if there's just one element.
         // Wrapping them in oneOf, make us lose details on validation errors so just do it if necessary.
         ArrayNode messagesNode = (ArrayNode) messageNode;
         if (messagesNode.size() > 1) {
            return buildOneOfMessagesAvroSchemas(specificationNode, (ArrayNode) messageNode, schemaMap);
         }
         return buildSingleMessageAvroSchema(followRefIfAny(messagesNode.get(0), specificationNode), specificationNode,
               schemaMap);
      } else if (messageNode.has(ONE_OF_STRUCT)) {
         ArrayNode oneOfMessageNode = (ArrayNode) messageNode.get(ONE_OF_STRUCT);
         return buildOneOfMessagesAvroSchemas(specificationNode, oneOfMessageNode, schemaMap);
      } else {
         return buildSingleMessageAvroSchema(messageNode, specificationNode, schemaMap);
      }
   }

   /** Build an array of Avro schemas for messages expressed as a direct oneOf structure. */
   private static Schema buildOneOfMessagesAvroSchemas(JsonNode specificationNode, ArrayNode oneOfMessageNode,
         SchemaMap schemaMap) throws AsyncAPISchemaException {
      // Initialize a oneOf schema with array.
      Schema[] schemas = new Schema[oneOfMessageNode.size()];

      // Extract the schema payload for each alternative of the message.
      for (int i = 0; i < oneOfMessageNode.size(); i++) {
         JsonNode altMessageNode = oneOfMessageNode.get(i);
         // Extract a schema for each messages
         altMessageNode = followRefIfAny(altMessageNode, specificationNode);
         schemas[i] = buildSingleMessageAvroSchema(altMessageNode, specificationNode, schemaMap);
      }

      try {
         return Schema.createUnion(schemas);
      } catch (AvroRuntimeException are) {
         // Union creation fails on duplicated or nested union branches. Turn this unchecked exception into the
         // checked one callers already handle so that it is reported as a validation error.
         log.info("oneOf message schemas cannot be assembled into an Avro union: {}", are.getMessage());
         throw new AsyncAPISchemaException("oneOf messages cannot be assembled into an Avro union: " + are.getMessage(),
               are);
      }
   }

   /**
    * Check whether a {@code schemaFormat} media type denotes an Apache Avro schema. Matching is done on a prefix basis
    * because AsyncAPI documents use several flavors of that media type: the plain {@code application/vnd.apache.avro}
    * one, the {@code +json} variant and an optional {@code ;version=} parameter.
    * @param schemaFormat The schemaFormat value found in an AsyncAPI document, may be null
    * @return True if this schemaFormat denotes an Avro schema, false otherwise
    */
   public static boolean isAvroSchemaFormat(String schemaFormat) {
      return schemaFormat != null && schemaFormat.startsWith(AVRO_SCHEMA_FORMAT_PREFIX);
   }

   /**
    * Unwrap the AsyncAPI v3 <i>Multi Format Schema Object</i> that may be used as a message payload. In AsyncAPI v3, an
    * Avro payload is expressed as
    * <code>payload: {schemaFormat: "application/vnd.apache.avro;version=1.9.0", schema: {...}}</code> whereas in
    * AsyncAPI v2 the payload node is directly the Avro schema. This returns the inner {@code schema} node when such a
    * wrapper is detected and the unchanged node otherwise, which keeps the AsyncAPI v2 handling untouched.
    * @param payloadNode         The message payload node to inspect
    * @param messageSchemaFormat The schemaFormat declared at the Message Object level, may be null
    * @return The effective Avro schema node for this payload
    */
   public static JsonNode unwrapAvroMultiFormatSchema(JsonNode payloadNode, String messageSchemaFormat) {
      // A Multi Format Schema Object only holds 'schemaFormat' and 'schema' members while an Avro schema node always
      // carries a 'type': its presence tells us we're on an AsyncAPI v2 payload that must be left untouched.
      if (!payloadNode.has(SCHEMA_ELEMENT) || payloadNode.has(TYPE_ELEMENT)) {
         return payloadNode;
      }

      // schemaFormat is mandatory on a Multi Format Schema Object but tolerate documents hoisting it on the Message
      // Object as AsyncAPI v2 was doing.
      String schemaFormat = messageSchemaFormat;
      if (payloadNode.has(SCHEMA_FORMAT_ELEMENT)) {
         schemaFormat = payloadNode.path(SCHEMA_FORMAT_ELEMENT).asText();
      }

      if (isAvroSchemaFormat(schemaFormat)) {
         log.debug("Unwrapping an AsyncAPI v3 Multi Format Schema Object having format {}", schemaFormat);
         return payloadNode.path(SCHEMA_ELEMENT);
      }
      return payloadNode;
   }

   /** Build an Avro schema for a single message definition. */
   private static Schema buildSingleMessageAvroSchema(JsonNode messageNode, JsonNode specificationNode,
         SchemaMap schemaMap) throws AsyncAPISchemaException {
      // Check that message node has a payload attribute.
      if (!messageNode.has(ASYNC_SCHEMA_PAYLOAD_ELEMENT)) {
         log.debug("messageNode {} has no 'payload' attribute", messageNode);
         throw new AsyncAPISchemaException("message definition has no valid payload in AsyncAPI specification");
      }

      // Remember the Message Object schemaFormat: AsyncAPI v2 declares it there and some v3 documents still do.
      String messageSchemaFormat = messageNode.has(SCHEMA_FORMAT_ELEMENT)
            ? messageNode.path(SCHEMA_FORMAT_ELEMENT).asText()
            : null;

      // Navigate to payload definition, unwrapping the AsyncAPI v3 Multi Format Schema Object if any so that the rest
      // of the processing is AsyncAPI version agnostic.
      JsonNode payloadNode = unwrapAvroMultiFormatSchema(messageNode.path(ASYNC_SCHEMA_PAYLOAD_ELEMENT),
            messageSchemaFormat);

      // Payload node can be just a reference to another schema... But in the case of Avro, this is an external schema
      // as #/components/schemas can only hold JSON schemas. So we have to use a registry for resolving and accessing
      // this Avro schema. We'll have to build an Avro Schema either from payload content or registry content.
      String schemaContent = null;

      if (payloadNode.has(REF_ELEMENT)) {
         String ref = payloadNode.path(REF_ELEMENT).asText();
         if (ref.startsWith("#")) {
            // An internal reference: the Avro schema is inlined somewhere else within the document.
            log.debug("Following an internal reference to an Avro schema: {}", ref);
            schemaContent = unwrapAvroMultiFormatSchema(followRefIfAny(payloadNode, specificationNode),
                  messageSchemaFormat).toString();
         } else {
            // Remove trailing anchor marker if any.
            // './user-signedup.avsc#/User' => './user-signedup.avsc'
            log.debug("Looking for an external Avro schema in registry: {}", ref);
            if (ref.contains("#")) {
               ref = ref.substring(0, ref.indexOf("#"));
            }
            if (schemaMap != null) {
               schemaContent = schemaMap.getSchemaEntry(ref);
            }
            if (schemaContent == null) {
               log.info("No schema content found in SchemaMap. {} is not found", ref);
               throw new AsyncAPISchemaException("no schema content found for " + ref + " in used SchemaMap.");
            }
         }
      } else {
         // Schema is specified within the payload definition.
         schemaContent = payloadNode.toString();
      }

      // Now build and return the schema.
      try {
         return AvroUtil.getSchema(schemaContent);
      } catch (AvroRuntimeException are) {
         // SchemaParseException is unchecked: turn it into the checked exception callers already handle so that an
         // invalid Avro schema is reported as a validation error instead of killing the calling thread.
         log.info("Avro schema content cannot be parsed: {}", are.getMessage());
         throw new AsyncAPISchemaException("Avro schema cannot be parsed: " + are.getMessage(), are);
      }
   }

   /** Check if a node has a reference and follow it to target node in the document. */
   public static JsonNode followRefIfAny(JsonNode referencableNode, JsonNode documentRoot) {
      if (referencableNode.has("$ref")) {
         String ref = referencableNode.path("$ref").asText();
         return followRefIfAny(documentRoot.at(ref.substring(1)), documentRoot);
      }
      return referencableNode;
   }
}
