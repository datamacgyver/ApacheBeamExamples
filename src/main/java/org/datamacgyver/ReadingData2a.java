package org.datamacgyver;

public class ReadingData2a {
}


//    public static void runCsvToAvro(SampleOptions options)
//            throws IOException, IllegalArgumentException {
//        FileSystems.setDefaultPipelineOptions(options);
//        // Get Avro Schema
//        String schemaJson = getSchema(options.getAvroSchema());
//        Schema schema = new Schema.Parser().parse(schemaJson);
//        // Check schema field types before starting the Dataflow job
//        checkFieldTypes(schema);
//        // Create the Pipeline object with the options we defined above.
//        Pipeline pipeline = Pipeline.create(options);
//        // Convert CSV to Avro
//        pipeline.apply("Read CSV files", TextIO.read().from(options.getInputFile()))
//                .apply("Convert CSV to Avro formatted data",
//                        ParDo.of(new ConvertCsvToAvro(schemaJson, options.getCsvDelimiter())))
//                .setCoder(AvroCoder.of(GenericRecord.class, schema))
//                .apply("Write Avro formatted data", AvroIO.writeGenericRecords(schemaJson)
//                        .to(options.getOutput()).withCodec(CodecFactory.snappyCodec()).withSuffix(".avro"));
//        // Run the pipeline.
//        pipeline.run().waitUntilFinish();
//    }