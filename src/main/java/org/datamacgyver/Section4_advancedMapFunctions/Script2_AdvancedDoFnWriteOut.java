package org.datamacgyver.Section4_advancedMapFunctions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.datamacgyver.Section1_ReadFiles.ReadingDataParquet;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;

public class Script2_AdvancedDoFnWriteOut {

    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        PCollection<GenericRecord> readParquet = p.apply("ReadLines field", ParquetIO.read(ReadingDataParquet.avroSchema).from(inFileParquet));

        //This one gets heavy, steel yourself and scroll down!!
        readParquet
                .apply("Map records to strings", ParDo.of(new GetCsvLines(ReadingDataParquet.avroSchema)))
                .apply("Write CSV formatted data", TextIO.write().to("exampleOutputs/csvTest").withSuffix(".csv"));

        p.run().waitUntilFinish();
    }


    //The example I'm using here is kinda made up. You would probably do this very differently in practice. But
    // the mechanics of this fit what I want to show you without any extra setup. The main problem with this approach
    // is that all the records for one bundle need to be aggregates. That could memory out pretty easily! I've also
    // taken some liberties with how this is structured. Again, more to make the required points than providing a
    // good practice for anything. As in the earlier example, the only essential bit is @ProcessElement
    // notes from https://stackoverflow.com/a/50068377/6814598

    //Important definition: Bundle - A collection of records that are processed together. Many bundles are run in
    //parallel but the records within them (I think) are run sequentially.
    static class GetCsvLines extends DoFn<GenericRecord, String> {

        // Creating Global Class variables. These are made before serialisation so will exist for all workers.
        StringBuilder csvContent;
        List<String> schema = new ArrayList<>();

        //As above, things done in a class constructor are done on the original class before it's serialsied and
        //sent ot the workers.
        public GetCsvLines(Schema schema){
            //Schema objects can't be serialised and all we want is the headers for the csv. As such we parse it to
            // a List<String>
            for (Schema.Field i : schema.getFields()) {
                this.schema.add(i.name());
            }
        }

        //@Setup is where you would want to do costly setup such as parsing config files or establishing connections
        // to shared resources. It's assumed that this will persist between bundles so no actual processing can live here.
        // The opposite to setup is @Teardown.
        @Setup
        public void setup(){
            csvContent = new StringBuilder();
        }

        //This is startup tasks at the start of this bundle of operations. Run once per bundle so can start off your
        // data processing
        @StartBundle
        public void startBundle(StartBundleContext c) throws Exception {
            // This bit lives here and not the constructor as I want each bundle to have it's own header.
            csvContent.append(Joiner.on(",").join(schema));
            csvContent.append("\n");
        }

        // As in Script one, these are the things you do on every record. For me, I just create CSV rows.
        @ProcessElement
        public void processElement(@Element GenericRecord rw) {      //I don't need an output variable here as I'm wrapping it up when I finish the bundle.
            boolean first = true;
            for (Schema.Field i : rw.getSchema().getFields()) {
                if (first) {
                    first = false;
                    csvContent.append("\"");
                    csvContent.append(rw.get(i.name()));
                    csvContent.append("\"");
                }
                else {
                    csvContent.append(",");
                }
            }
            csvContent.append("\n");
            //out.output(x)                                         //I don't need an output variable here as I'm wrapping it up when I finish the bundle.
        }

        //This is run when your bundle is complete, it can form part of the transfrom process but finalises it.
        // For me, I now create a final string and hand it to output.
        @FinishBundle
        public void finishBundle(FinishBundleContext c) {
            c.output(csvContent.toString(), Instant.now(), GlobalWindow.INSTANCE);
        }

        //As with setup this is assumed to be shared accross bundles of records so will persist. As such, no transforms
        //can happen here.
        @Teardown
        public void teardown(){
            //I don't have anything to do here but this would be things like closing database connections.
        }
    }

}
