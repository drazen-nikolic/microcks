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
package io.github.microcks.util;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Helper class using utility methods for converting Avro format from and to JSON.
 * @author laurent
 */
public class AvroUtil {

   /** A simple logger for diagnostic messages. */
   private static final Logger log = LoggerFactory.getLogger(AvroUtil.class);

   private AvroUtil() {
      // Private constructor to hide implicit public one.
   }

   /**
    * Convert a Avro schema string into a Schema object.
    * @param avroSchema String representation of an Avro Schema to use for conversion
    * @return The Avro Schema to use
    * @throws org.apache.avro.SchemaParseException if the schema is not valid
    */
   public static Schema getSchema(String avroSchema) {
      return new Schema.Parser().parse(avroSchema);
   }

   /**
    * Convert a JSON string into an Avro binary representation using specified schema.
    * @param json       A JSON string to convert to Avro
    * @param avroSchema String representation of an Avro Schema to use for conversion
    * @return The Avro binary representation of JSON
    * @throws AvroTypeException if there's a mismatch between JSON string and Avro Schema
    * @throws IOException       if something goes wrong during conversion
    */
   public static byte[] jsonToAvro(String json, String avroSchema) throws AvroTypeException, IOException {
      return jsonToAvro(json, getSchema(avroSchema));
   }

   /**
    * Convert a JSON string into an Avro binary representation using specified schema.
    * @param json       A JSON string to convert to Avro
    * @param avroSchema The Avro Schema to use for conversion
    * @return The Avro binary representation of JSON
    * @throws AvroTypeException if there's a mismatch between JSON string and Avro Schema
    * @throws IOException       if something goes wrong during conversion
    */
   public static byte[] jsonToAvro(String json, Schema avroSchema) throws AvroTypeException, IOException {
      if (avroSchema.isUnion()) {
         // If the schema is a union, we need to find the right schema to use.
         List<String> reasons = new ArrayList<>();
         for (Schema schema : avroSchema.getTypes()) {
            try {
               return jsonToAvro(json, schema);
            } catch (IOException | RuntimeException e) {
               // This branch does not match, record why and try the next schema.
               log.debug("Union branch {} does not match JSON data: {}", schema.getFullName(), e.getMessage());
               reasons.add(schema.getFullName() + ": " + e.getMessage());
            }
         }
         throw noUnionBranchMatches("JSON data", reasons);
      }
      // Prepare reader an input stream from Json string.
      GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);
      InputStream input = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
      JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(avroSchema, input);

      // Prepare write and output stream to produce binary encoding.
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      GenericDatumWriter<Object> writer = new GenericDatumWriter<>(avroSchema);
      Encoder e = EncoderFactory.get().binaryEncoder(baos, null);

      // Read the data into a GenericRecord.
      GenericRecord datum = reader.read(null, jsonDecoder);

      // Write the GenericRecord to the Avro binary.
      writer.write(datum, e);
      e.flush();

      return baos.toByteArray();
   }

   /**
    * Convert a JSON string into an Avro GenericRecord object using specified schema.
    * @param json       A JSON string to convert to Avro
    * @param avroSchema String representation of an Avro Schema to use for conversion
    * @return The GenericRecord representation of JSON
    * @throws AvroTypeException if there's a mismatch between JSON string and Avro Schema
    * @throws IOException       if something goes wrong during conversion
    */
   public static GenericRecord jsonToAvroRecord(String json, String avroSchema) throws AvroTypeException, IOException {
      return jsonToAvroRecord(json, getSchema(avroSchema));
   }

   /**
    * Convert a JSON string into an Avro GenericRecord object using specified schema.
    * @param json       A JSON string to convert to Avro
    * @param avroSchema The Avro Schema to use for conversion
    * @return The GenericRecord representation of JSON
    * @throws AvroTypeException if there's a mismatch between JSON string and Avro Schema
    * @throws IOException       if something goes wrong during conversion
    */
   public static GenericRecord jsonToAvroRecord(String json, Schema avroSchema) throws AvroTypeException, IOException {
      if (avroSchema.isUnion()) {
         // If the schema is a union, we need to find the right schema to use.
         List<String> reasons = new ArrayList<>();
         for (Schema schema : avroSchema.getTypes()) {
            try {
               return jsonToAvroRecord(json, schema);
            } catch (IOException | RuntimeException e) {
               // This branch does not match, record why and try the next schema.
               log.debug("Union branch {} does not match JSON data: {}", schema.getFullName(), e.getMessage());
               reasons.add(schema.getFullName() + ": " + e.getMessage());
            }
         }
         throw noUnionBranchMatches("JSON data", reasons);
      }

      // Prepare reader an input stream from Json string.
      GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);
      InputStream input = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
      JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(avroSchema, input);

      return reader.read(null, jsonDecoder);
   }

   /**
    * Convert an Avro binary representation into a JSON string using specified schema.
    * @param avroBinary An Avro binary representation to convert in JSON string
    * @param avroSchema The Avro Schema to use for conversion
    * @return The JSON string representing Avro binary
    * @throws AvroTypeException if there's a mismatch between Avro binary and Schema
    * @throws IOException       if something goes wrong during conversion
    */
   public static String avroToJson(byte[] avroBinary, String avroSchema) throws AvroTypeException, IOException {
      return avroToJson(avroBinary, getSchema(avroSchema));
   }

   /**
    * Convert an Avro binary representation into a JSON string using specified schema.
    * @param avroBinary An Avro binary representation to convert in JSON string
    * @param avroSchema The Avro Schema to use for conversion
    * @return The JSON string representing Avro binary
    * @throws AvroTypeException if there's a mismatch between Avro binary and Schema
    * @throws IOException       if something goes wrong during conversion
    */
   public static String avroToJson(byte[] avroBinary, Schema avroSchema) throws AvroTypeException, IOException {
      if (avroSchema.isUnion()) {
         // Datum may have been written using the union as writer schema: honor the branch index first.
         Optional<Object> unionDatum = readUnionEncodedDatum(avroBinary, avroSchema);
         if (unionDatum.isPresent()) {
            return GenericData.get().toString(unionDatum.get());
         }
         // Fallback: datum may have been written using one of the union branches as a standalone writer schema.
         List<String> reasons = new ArrayList<>();
         for (Schema schema : avroSchema.getTypes()) {
            try {
               return avroToJson(avroBinary, schema);
            } catch (IOException | RuntimeException e) {
               // This branch does not match, record why and try the next schema.
               log.debug("Union branch {} cannot read Avro binary: {}", schema.getFullName(), e.getMessage());
               reasons.add(schema.getFullName() + ": " + e.getMessage());
            }
         }
         throw noUnionBranchMatches("Avro binary data", reasons);
      }
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
      Decoder decoder = DecoderFactory.get().binaryDecoder(avroBinary, null);

      GenericRecord genRecord = datumReader.read(null, decoder);
      return genRecord.toString();
   }

   /**
    * Convert an Avro binary representation into an Avro GenericRecord object using specified schema.
    * @param avroBinary An Avro binary representation to convert in record
    * @param avroSchema The Avro Schema to use for conversion
    * @return The JSON string representing Avro binary
    * @throws AvroTypeException if there's a mismatch between Avro binary and Schema
    * @throws IOException       if something goes wrong during conversion
    */
   public static GenericRecord avroToAvroRecord(byte[] avroBinary, String avroSchema)
         throws AvroTypeException, IOException {
      return avroToAvroRecord(avroBinary, getSchema(avroSchema));
   }

   /**
    * Convert an Avro binary representation into an Avro GenericRecord object using specified schema.
    * @param avroBinary An Avro binary representation to convert in record
    * @param avroSchema The Avro Schema to use for conversion
    * @return The JSON string representing Avro binary
    * @throws AvroTypeException if there's a mismatch between Avro binary and Schema
    * @throws IOException       if something goes wrong during conversion
    */
   public static GenericRecord avroToAvroRecord(byte[] avroBinary, Schema avroSchema)
         throws AvroTypeException, IOException {
      if (avroSchema.isUnion()) {
         // Datum may have been written using the union as writer schema: honor the branch index first.
         Optional<Object> unionDatum = readUnionEncodedDatum(avroBinary, avroSchema);
         if (unionDatum.isPresent() && unionDatum.get() instanceof GenericRecord unionRecord) {
            return unionRecord;
         }
         // Fallback: datum may have been written using one of the union branches as a standalone writer schema.
         List<String> reasons = new ArrayList<>();
         for (Schema schema : avroSchema.getTypes()) {
            try {
               return avroToAvroRecord(avroBinary, schema);
            } catch (IOException | RuntimeException e) {
               // This branch does not match, record why and try the next schema.
               log.debug("Union branch {} cannot read Avro binary: {}", schema.getFullName(), e.getMessage());
               reasons.add(schema.getFullName() + ": " + e.getMessage());
            }
         }
         throw noUnionBranchMatches("Avro binary data", reasons);
      }
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
      Decoder decoder = DecoderFactory.get().binaryDecoder(avroBinary, null);

      return datumReader.read(null, decoder);
   }

   /**
    * Validate that a datum object (typically a GenericRecord read somewhere but the method signature is loosely coupled
    * to make it recursive friendly) is compliant with an Avro schema.
    * @param schema The Avro Schema to validate datum against
    * @param datum  The Object datum to validate
    * @return True if the object is compliant with supplied schema, false otherwise.
    */
   public static boolean validate(Schema schema, Object datum) {
      switch (schema.getType()) {
         case RECORD:
            if (datum instanceof GenericRecord genericRecord) {
               for (Schema.Field f : schema.getFields()) {
                  if (!genericRecord.hasField(f.name()))
                     return false;
                  if (!validate(f.schema(), genericRecord.get(f.pos())))
                     return false;
               }
               return true;
            }
            return GenericData.get().validate(schema, datum);
         default:
            return GenericData.get().validate(schema, datum);
      }
   }

   /**
    * Get validation errors of a datum object regarding Avro schema.
    * @param schema    The Schema to check datum object against
    * @param datum     The datum object to validate
    * @param fieldName The name of the field we're currently validating
    * @return A list of String representing validation errors. List may be empty if no error found.
    */
   public static List<String> getValidationErrors(Schema schema, Object datum, String... fieldName) {
      List<String> errors = new ArrayList<>();

      switch (schema.getType()) {
         case RECORD:
            if (datum instanceof GenericRecord genericRecord) {
               for (Schema.Field f : schema.getFields()) {
                  // Check for defined and required field.
                  if (!genericRecord.hasField(f.name()) && !f.hasDefaultValue()) {
                     errors.add("Required field " + f.name() + " cannot be found in record");
                  } else if (genericRecord.hasField(f.name())) {
                     // Now add errors for each field if defined at the record level.
                     errors.addAll(getValidationErrors(f.schema(), genericRecord.get(f.pos()), f.name()));
                  }
               }
            }
            break;
         case ENUM:
            if (datum == null || !schema.hasEnumSymbol(datum.toString()))
               errors.add(datum + " enum value is not defined in schema");
            break;
         case ARRAY:
            if (!(datum instanceof Collection<?> collection)) {
               errors.add(fieldNameOrDefault(fieldName) + " is not a valid array");
            } else {
               // Now add errors for each element, keeping the enclosing field name for diagnostics.
               for (Object element : collection) {
                  errors.addAll(getValidationErrors(schema.getElementType(), element, fieldName));
               }
            }
            break;
         case STRING:
            if (!(datum instanceof CharSequence))
               errors.add(fieldNameOrDefault(fieldName) + " is not a string");
            break;
         case BYTES:
            if (!(datum instanceof ByteBuffer))
               errors.add(fieldNameOrDefault(fieldName) + " is not bytes");
            break;
         case INT:
            if (!(datum instanceof Integer))
               errors.add(fieldNameOrDefault(fieldName) + " is not an integer");
            break;
         case LONG:
            if (!(datum instanceof Long))
               errors.add(fieldNameOrDefault(fieldName) + " is not a long");
            break;
         case FLOAT:
            if (!(datum instanceof Float))
               errors.add(fieldNameOrDefault(fieldName) + " is not a float");
            break;
         case DOUBLE:
            if (!(datum instanceof Double))
               errors.add(fieldNameOrDefault(fieldName) + " is not a double");
            break;
         case BOOLEAN:
            if (!(datum instanceof Boolean))
               errors.add(fieldNameOrDefault(fieldName) + " is not a boolean");
            break;
         case UNION:
            // Get validation errors for each type in union, keeping the enclosing field name for diagnostics.
            for (Schema unionSchema : schema.getTypes()) {
               errors.addAll(getValidationErrors(unionSchema, datum, fieldName));
            }
            break;
      }
      return errors;
   }

   /**
    * Try reading an Avro binary that has been written using {@code unionSchema} as writer schema, thus starting with a
    * branch index. Returns an empty Optional if the binary cannot be fully read that way, so that the caller can fall
    * back to reading it against each branch of the union taken as a standalone schema.
    * @param avroBinary  The Avro binary representation to read
    * @param unionSchema The union Avro Schema to use for reading
    * @return The datum read, or an empty Optional if binary is not a union encoded datum for this union
    */
   private static Optional<Object> readUnionEncodedDatum(byte[] avroBinary, Schema unionSchema) {
      try {
         BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroBinary, null);
         GenericDatumReader<Object> reader = new GenericDatumReader<>(unionSchema);
         Object datum = reader.read(null, decoder);
         // Only trust this interpretation if the whole binary has been consumed: the first byte of a datum written
         // with a bare branch schema may accidentally look like a valid union branch index and the Avro binary
         // decoder does not complain about trailing bytes.
         if (datum != null && decoder.isEnd()) {
            return Optional.of(datum);
         }
         log.debug("Union encoded reading left unconsumed bytes, will try reading each union branch");
      } catch (IOException | RuntimeException e) {
         log.debug("Union encoded reading failed ({}), will try reading each union branch", e.getMessage());
      }
      return Optional.empty();
   }

   /** Build an AvroTypeException gathering the reasons why no branch of a union schema matched. */
   private static AvroTypeException noUnionBranchMatches(String what, List<String> reasons) {
      return new AvroTypeException("No schema in union matches " + what + " [" + String.join(", ", reasons) + "]");
   }

   /** Safely get the current field name, as the varargs may be empty on top-level or recursive calls. */
   private static String fieldNameOrDefault(String... fieldName) {
      return (fieldName != null && fieldName.length > 0) ? fieldName[0] : "Value";
   }
}
