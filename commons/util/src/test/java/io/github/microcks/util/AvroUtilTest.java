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
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test case method for AvroUtil class.
 * @author laurent
 */
class AvroUtilTest {

   @Test
   void testAvroBasics() {
      Schema schema = null;

      try {
         // Load schema from file.
         schema = new Schema.Parser()
               .parse(new File("target/test-classes/io/github/microcks/util/user-signedup-bad.avsc"));

         GenericRecord user1 = new GenericData.Record(schema);
         user1.put("name", "Laurent");
         user1.put("email", "laurent@microcks.io");
         user1.put("age", 41);

         GenericRecord user2 = new GenericData.Record(schema);
         user2.put("name", "John");
         user2.put("email", "john@microcks.io");
         user2.put("age", 23);

         // Serialize using Json encoding.
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         PrintStream out = new PrintStream(new BufferedOutputStream(baos));

         DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
         Encoder encoder = EncoderFactory.get().jsonEncoder(schema, out, false);
         datumWriter.write(user1, encoder);
         datumWriter.write(user2, encoder);
         encoder.flush();
         String jsonEncoding = baos.toString("UTF-8");
         System.err.println("jsonEncoding: \n" + jsonEncoding);

         // Serialize using binary encoding.
         baos = new ByteArrayOutputStream();
         out = new PrintStream(new BufferedOutputStream(baos));
         encoder = EncoderFactory.get().binaryEncoder(out, null);
         datumWriter.write(user1, encoder);
         datumWriter.write(user2, encoder);
         encoder.flush();
         byte[] binaryRepresentation = baos.toByteArray();
         String binaryEncoding = new String(binaryRepresentation, "UTF-8");
         System.err.println("\nbinaryEncoding: \n" + binaryEncoding);

         // Deserialize from binary encoding representation.
         DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
         GenericRecord user = null;
         Decoder decoder = DecoderFactory.get().binaryDecoder(binaryRepresentation, null);

         try {
            while (true) {
               user = datumReader.read(user, decoder);
               System.err.println("User from binary representation: " + user);
            }
         } catch (EOFException eofException) {
            // Nothing to do here, just exit the while loop.
         }
      } catch (Exception e) {
         fail("Exception should not be thrown");
      }
   }

   @Test
   void testJsonToAvro() {
      String jsonText = "{\"name\":\"Laurent Broudoux\", \"email\":\"laurent@microcks.io\", \"age\":41}";

      try {
         // Load schema from file.
         Schema schema = new Schema.Parser()
               .parse(new File("target/test-classes/io/github/microcks/util/user-signedup-bad.avsc"));

         // Convert back and forth to and from JSON.
         byte[] avroBinary = AvroUtil.jsonToAvro(jsonText, schema);
         System.err.println("binaryEncoding: \n" + new String(avroBinary, "UTF-8"));
         String jsonRepresentation = AvroUtil.avroToJson(avroBinary, schema);
         System.err.println("\njsonRepresentation: \n" + jsonRepresentation);

         assertTrue(jsonRepresentation.contains("\"Laurent Broudoux\""));
         assertTrue(jsonRepresentation.contains("\"laurent@microcks.io\""));
         assertTrue(jsonRepresentation.contains("41"));

         // Deserialize from binary encoding representation.
         DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
         GenericRecord user = null;
         Decoder decoder = DecoderFactory.get().binaryDecoder(avroBinary, null);

         try {
            while (true) {
               user = datumReader.read(user, decoder);
               System.err.println("\nUser from binary representation: \n" + user.toString());
            }
         } catch (EOFException eofException) {
            // Nothing to do here, just exit the while loop.
         }
         assertEquals("Laurent Broudoux", user.get("name").toString());
         assertEquals("laurent@microcks.io", user.get("email").toString());
         assertEquals(Integer.valueOf(41), (Integer) user.get("age"));
      } catch (Exception e) {
         fail("Exception should not be thrown");
      }
   }

   @Test
   void testJsonToAvroUnionSchema() {
      String jsonText = "{\"name\":\"Tigresse\"}";

      Schema cat = SchemaBuilder.record("Cat").fields().requiredString("name").endRecord();
      Schema chat = SchemaBuilder.record("Chat").fields().requiredString("nom").endRecord();
      Schema union = SchemaBuilder.unionOf().type(cat).and().type(chat).endUnion();

      try {
         byte[] avroBinary = AvroUtil.jsonToAvro(jsonText, union);
         System.err.println("binaryEncoding: \n" + new String(avroBinary, StandardCharsets.UTF_8));
         String jsonRepresentation = AvroUtil.avroToJson(avroBinary, union);
         System.err.println("\njsonRepresentation: \n" + jsonRepresentation);

         assertTrue(jsonRepresentation.contains("\"Tigresse\""));

         // Deserialize from binary encoding representation.
         GenericRecord catRecord = AvroUtil.avroToAvroRecord(avroBinary, union);
         System.err.println("\nCat from binary representation: \n" + catRecord.toString());

         assertEquals("Tigresse", catRecord.get("name").toString());
      } catch (Exception e) {
         fail("Exception should not be thrown");
      }
   }

   @Test
   void testJsonToAvroRecord() {
      String jsonText = "{\"name\":\"Laurent Broudoux\", \"email\":\"laurent@microcks.io\", \"age\":42}";

      try {
         // Load schema from file.
         Schema schema = new Schema.Parser()
               .parse(new File("target/test-classes/io/github/microcks/util/user-signedup-bad.avsc"));

         GenericRecord genericRecord = AvroUtil.jsonToAvroRecord(jsonText, schema);
         assertEquals("Laurent Broudoux", genericRecord.get("name").toString());
         assertEquals("laurent@microcks.io", genericRecord.get("email").toString());
         assertEquals(Integer.valueOf(42), Integer.valueOf(genericRecord.get("age").toString()));
      } catch (Exception e) {
         fail("Exception should not be thrown");
      }
   }

   @Test
   void testAvroBinaryReadingFailure() {
      String jsonText = "{\"name\":\"Laurent Broudoux\", \"email\":\"laurent@microcks.io\", \"age\":41}";

      try {
         // Load schema from file.
         Schema writeSchema = new Schema.Parser()
               .parse(new File("target/test-classes/io/github/microcks/util/user-signedup-bad.avsc"));

         // Convert back and forth to and from JSON.
         byte[] avroBinary = AvroUtil.jsonToAvro(jsonText, writeSchema);
         System.err.println("binaryEncoding: \n" + new String(avroBinary, "UTF-8"));

         Schema readSchema = new Schema.Parser()
               .parse(new File("target/test-classes/io/github/microcks/util/user-signedup.avsc"));
         String jsonRepresentation = AvroUtil.avroToJson(avroBinary, readSchema);
         System.err.println("\njsonRepresentation: \n" + jsonRepresentation);

         GenericRecord genericRecord = AvroUtil.avroToAvroRecord(avroBinary, readSchema);
         System.err.println(AvroUtil.validate(readSchema, genericRecord));

      } catch (Exception e) {
         fail("Exception should not be thrown");
      }
   }

   @Test
   void testValidate() {
      Schema v1Schema = SchemaBuilder.record("User").fields().requiredString("name").requiredInt("age").endRecord();
      Schema v2Schema = SchemaBuilder.record("User").fields().requiredString("fullName").requiredInt("age")
            .optionalString("email").endRecord();

      GenericRecord userv1 = new GenericData.Record(v1Schema);
      userv1.put("name", "Laurent");
      userv1.put("age", 42);

      assertFalse(AvroUtil.validate(v2Schema, userv1));
      // The Avro validate method fails because it does not validate the field name
      // just the position. This make it not usable in our context.
      //assertFalse(GenericData.get().validate(v2Schema, userv1));

      List<String> errors = AvroUtil.getValidationErrors(v2Schema, userv1);
      assertEquals(1, errors.size());
      assertEquals("Required field fullName cannot be found in record", errors.get(0));

      GenericRecord userv2 = new GenericData.Record(v2Schema);
      userv2.put("fullName", "Laurent Broudoux");
      userv2.put("age", 42);

      assertTrue(AvroUtil.validate(v2Schema, userv2));
   }

   @Test
   void testAvroSchemaCompatibility() {
      Schema v1Schema = SchemaBuilder.record("User").fields().requiredString("name").requiredInt("age").endRecord();
      Schema v2Schema = SchemaBuilder.record("User").fields().requiredString("fullName").requiredInt("age")
            .optionalString("email").endRecord();

      GenericRecord userv1 = new GenericData.Record(v1Schema);
      userv1.put("name", "Laurent");
      userv1.put("age", 42);

      SchemaCompatibility.SchemaPairCompatibility compatibility = SchemaCompatibility
            .checkReaderWriterCompatibility(userv1.getSchema(), v2Schema);
      SchemaCompatibility.checkReaderWriterCompatibility(userv1.getSchema(), v2Schema).getResult()
            .getIncompatibilities().stream()
            .forEach(incompatibility -> System.err.println(incompatibility.getMessage()));
      assertEquals(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE, compatibility.getType());
   }

   @Test
   void testValidUnionSchema() {
      Schema cat = SchemaBuilder.record("Cat").fields().requiredString("name").endRecord();
      Schema chat = SchemaBuilder.record("Chat").fields().requiredString("nom").endRecord();

      Schema union = SchemaBuilder.unionOf().type(cat).and().type(chat).endUnion();

      GenericRecord tigresse = new GenericData.Record(cat);
      tigresse.put("name", "Tigresse");

      // Assert that the record is valid against the union schema.
      assertTrue(AvroUtil.validate(union, tigresse));
      List<String> errors = AvroUtil.getValidationErrors(union, tigresse);
      // There should still be one error regarding the Chat schema conformance.
      assertEquals(1, errors.size());
      assertEquals("Required field nom cannot be found in record", errors.getFirst());
   }

   @Test
   void testInvalidUnionSchema() {
      Schema cat = SchemaBuilder.record("Cat").fields().requiredString("name").requiredInt("age").endRecord();
      Schema dog = SchemaBuilder.record("Dog").fields().requiredString("name").requiredInt("age")
            .optionalString("fluff").endRecord();

      Schema union = SchemaBuilder.unionOf().type(cat).and().type(dog).endUnion();

      GenericRecord tigresse = new GenericData.Record(cat);
      tigresse.put("name", "Tigresse");
      tigresse.put("age", "12");

      // Assert that the record is not valid against the union schema.
      assertFalse(AvroUtil.validate(union, tigresse));
      List<String> errors = AvroUtil.getValidationErrors(union, tigresse);
      assertEquals(2, errors.size());
      for (String error : errors) {
         assertEquals("age is not an integer", error);
      }
   }

   /** Write a datum using the given writer schema. Passing a union makes Avro emit a leading branch index. */
   private static byte[] writeDatum(GenericRecord record, Schema writerSchema) throws Exception {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      new GenericDatumWriter<Object>(writerSchema).write(record, encoder);
      encoder.flush();
      return baos.toByteArray();
   }

   @Test
   void testAvroToAvroRecordWithUnionEncodedBinary() throws Exception {
      Schema cat = SchemaBuilder.record("Cat").fields().requiredString("name").endRecord();
      Schema chat = SchemaBuilder.record("Chat").fields().requiredString("nom").endRecord();
      Schema union = SchemaBuilder.unionOf().type(cat).and().type(chat).endUnion();

      GenericRecord tigresse = new GenericData.Record(cat);
      tigresse.put("name", "Tigresse");

      // Written with the union as writer schema, so the binary starts with a branch index.
      byte[] avroBinary = writeDatum(tigresse, union);

      GenericRecord record = AvroUtil.avroToAvroRecord(avroBinary, union);
      assertEquals("Cat", record.getSchema().getName());
      assertEquals("Tigresse", record.get("name").toString());
   }

   @Test
   void testAvroToAvroRecordWithUnionEncodedSecondBranch() throws Exception {
      Schema cat = SchemaBuilder.record("Cat").fields().requiredString("name").endRecord();
      Schema chat = SchemaBuilder.record("Chat").fields().requiredString("nom").endRecord();
      Schema union = SchemaBuilder.unionOf().type(cat).and().type(chat).endUnion();

      GenericRecord minou = new GenericData.Record(chat);
      minou.put("nom", "Minou");

      byte[] avroBinary = writeDatum(minou, union);

      // The branch index must be honored: without it, the datum would be read as a Cat.
      GenericRecord record = AvroUtil.avroToAvroRecord(avroBinary, union);
      assertEquals("Chat", record.getSchema().getName());
      assertEquals("Minou", record.get("nom").toString());
   }

   @Test
   void testAvroToJsonWithUnionEncodedBinary() throws Exception {
      Schema cat = SchemaBuilder.record("Cat").fields().requiredString("name").endRecord();
      Schema chat = SchemaBuilder.record("Chat").fields().requiredString("nom").endRecord();
      Schema union = SchemaBuilder.unionOf().type(cat).and().type(chat).endUnion();

      GenericRecord minou = new GenericData.Record(chat);
      minou.put("nom", "Minou");

      String jsonRepresentation = AvroUtil.avroToJson(writeDatum(minou, union), union);

      assertTrue(jsonRepresentation.contains("\"nom\""));
      assertTrue(jsonRepresentation.contains("\"Minou\""));
   }

   @Test
   void testAvroToAvroRecordWithBareBranchBinary() throws Exception {
      Schema cat = SchemaBuilder.record("Cat").fields().requiredString("name").endRecord();
      Schema chat = SchemaBuilder.record("Chat").fields().requiredString("nom").endRecord();
      Schema union = SchemaBuilder.unionOf().type(cat).and().type(chat).endUnion();

      GenericRecord tigresse = new GenericData.Record(cat);
      tigresse.put("name", "Tigresse");

      // Written with a single branch as writer schema: no leading branch index. This is what the Microcks
      // producer emits for union typed messages, so it must keep being readable.
      byte[] avroBinary = writeDatum(tigresse, cat);

      GenericRecord record = AvroUtil.avroToAvroRecord(avroBinary, union);
      assertEquals("Tigresse", record.get("name").toString());
   }

   @Test
   void testAvroToAvroRecordWithUnreadableBinaryThrowsAvroTypeException() {
      Schema cat = SchemaBuilder.record("Cat").fields().requiredString("name").endRecord();
      Schema chat = SchemaBuilder.record("Chat").fields().requiredString("nom").endRecord();
      Schema union = SchemaBuilder.unionOf().type(cat).and().type(chat).endUnion();

      byte[] garbage = new byte[] { -1, -1, -1, -1, -1, -1, -1, -1 };

      // Whatever the underlying Avro failure is, it must surface as a typed AvroTypeException naming the
      // branches that have been tried, never as an unchecked exception escaping to the caller.
      AvroTypeException ate = assertThrows(AvroTypeException.class, () -> AvroUtil.avroToAvroRecord(garbage, union));
      assertTrue(ate.getMessage().contains("Cat"));
      assertTrue(ate.getMessage().contains("Chat"));
   }

   @Test
   void testGetValidationErrorsOnTopLevelNonRecordSchema() {
      // Top-level call passes no field name: this must not raise an ArrayIndexOutOfBoundsException.
      List<String> errors = AvroUtil.getValidationErrors(SchemaBuilder.builder().stringType(), 42);
      assertFalse(errors.isEmpty());
   }

   @Test
   void testGetValidationErrorsOnArrayOfWrongTypedElements() {
      Schema schema = SchemaBuilder.record("Team").fields().name("members").type().array().items().stringType()
            .noDefault().endRecord();

      GenericRecord team = new GenericData.Record(schema);
      team.put("members", List.of(1, 2, 3));

      List<String> errors = AvroUtil.getValidationErrors(schema, team);
      assertFalse(errors.isEmpty());
   }

   @Test
   void testGetValidationErrorsOnTopLevelUnionOfPrimitives() {
      Schema union = SchemaBuilder.unionOf().stringType().and().intType().endUnion();

      List<String> errors = AvroUtil.getValidationErrors(union, Boolean.TRUE);
      assertFalse(errors.isEmpty());
   }
}
