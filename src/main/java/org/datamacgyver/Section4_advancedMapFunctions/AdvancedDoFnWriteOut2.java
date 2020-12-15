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
import org.datamacgyver.Section1_ReadFiles.ReadingDataParquet2;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;

public class AdvancedDoFnWriteOut2 {

    public static void main(String[] args) {
        String inFileParquet = "data/transformers.parquet";
        Pipeline p = Pipeline.create();

        PCollection<GenericRecord> readParquet = p.apply("ReadLines field", ParquetIO.read(ReadingDataParquet2.avroSchema).from(inFileParquet));

        readParquet
                .apply("Map records to strings", ParDo.of(new GetCsvLines(ReadingDataParquet2.avroSchema)))
                .apply("Write CSV formatted data", TextIO.write().to("exampleOutputs/csvTest").withSuffix(".csv"));

        p.run().waitUntilFinish();
    }


    //The example I'm using here is kinda made up. You would probably use a file writer to write a csv (one for a later
    //section). But the mechanics of this fit teh required learning outcomes very well. The main problem with this approach
    //is that all the records for one worker need to be stored in memory and then writen out at the end. That could break
    //pretty easily! I've also taken some liberties with how this is structured. Again, more to make the required points
    //than providing a good practice for writing CSVs. As in the earlier example, the only essential bit is @ProcessElement
    // notes from https://stackoverflow.com/a/50068377/6814598

    //Important definition: Bundle - A collection of records that are processed together. Many bundles are run in
    //parallel but the records within them (I think) are run sequentially.
    static class GetCsvLines extends DoFn<GenericRecord, String> {  //This is <in, out>

        StringBuilder csvContent;
        List<String> schema = new ArrayList<>();

        //things done in a class constructor are done on the original class before it's serialsied and
        //sent ot the workers.
        public GetCsvLines(Schema schema){
            //Schema objects can't be serialised so we parse to a list in the constructor.
            for (Schema.Field i : schema.getFields()) {
                this.schema.add(i.name());
            }
        }

        //This is where you would want to do costly setup such as parsing config files or establishing connections
        //to shared resources. It's assumed that this will persist between bundles so no actual processing can live here.
        //The opposite to setup is teardown.
        @Setup
        public void setup(){
            csvContent = new StringBuilder();
        }

        // This is startup tasks at the start of this bundle of operations. Run once per bundle.
        @StartBundle
        // This bit lives here and not the constructor as I want each bundle to have a header.
        public void startBundle(StartBundleContext c) throws Exception {
            // TODO: You could use joiner here: Joiner.on(",").join(list)
            boolean first = true;
            for (String i : schema) {
                if (!first) csvContent.append(",");
                csvContent.append("\"");
                csvContent.append(i);
                first = false;
                csvContent.append("\"");
            }
            csvContent.append("\n");
        }

        // This per-row operations. Run once per element (row).
        @ProcessElement
        public void processElement(@Element GenericRecord rw) {  //I don't need a per-row output here as I'm wrapping it up when I finish the bundle.
            // TODO: You could use joiner here: Joiner.on(",").join(list)
            boolean first = true;
            for (Schema.Field i : rw.getSchema().getFields()) {
                if (!first) csvContent.append(",");
                csvContent.append("\"");
                csvContent.append(rw.get(i.name()));
                first = false;
                csvContent.append("\"");
            }
            csvContent.append("\n");
            //Out is the per row return, as I'm bundling up to one csv I'm not using it. It'd look like:
            //out.output(x)
        }

        //This is run when your bundle is complete, it can form part of the transfrom process
        @FinishBundle
        public void finishBundle(FinishBundleContext c) {
            c.output(csvContent.toString(), Instant.now(), GlobalWindow.INSTANCE);
        }

        //as with setup this is assumed to be shared accross bundles of records so will persist. As such, no transforms
        //can happen here.
        @Teardown
        public void teardown(){
            //I don't have anything to do here but this would be things like closing database connections.
        }
    }

}
