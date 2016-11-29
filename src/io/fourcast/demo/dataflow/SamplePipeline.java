package io.fourcast.demo.dataflow;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.*;

import static com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
import static com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.to;

/**
 * Created by titatovenaar on 21/11/2016.
 */
public class SamplePipeline implements Serializable {
    private static String projectName = "deploy-test-2";

    public static final TupleTag<KV<Integer, Integer>> evenNumbers = new TupleTag<KV<Integer, Integer>>() {};
    public static final TupleTag<KV<Integer, Integer>> oddNumbers = new TupleTag<KV<Integer, Integer>>() {};
    public static final TupleTag<KV<Integer, Integer>> allNumbers = new TupleTag<KV<Integer, Integer>>() {};


    public static void main(String[] args) throws IOException {
        Pipeline p;
        if (args != null && args.length != 0) {
            p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
        } else {
            DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
            options.setRunner(DirectPipelineRunner.class); // local
            //  options.setRunner(DataflowPipelineRunner.class); // remote
            options.setProject(projectName);
            options.setStagingLocation("gs://" + projectName + "/dataflow-files/");
            options.setTempLocation("gs://" + projectName + "/dataflow-temp/");
            p = Pipeline.create(options);
        }
        new SamplePipeline(p);
    }

    public SamplePipeline(Pipeline p) {
        PCollectionTuple collections = p.apply(Create.of(3, 4, 5, 2, 4, 6)).apply("splitIntegers", ParDo
                .withOutputTags(allNumbers, TupleTagList.of(oddNumbers).and(evenNumbers))
                .of(new IntegerToKVFn()));

        collections.get(allNumbers).apply(GroupByKey.<Integer, Integer>create()).apply("number to TableRow", ParDo
                //.withOutputTags(PROJECTS, TupleTagList.of(SCENARIO))
                .of(new IntegerToTableRowFn()))
                .apply("Write Projects to BQ", to(bqTableRef())
                        .withSchema(getBQSchema())
                        .withWriteDisposition(WRITE_TRUNCATE)
                        .withCreateDisposition(CREATE_IF_NEEDED));

        collections.get(evenNumbers).apply(GroupByKey.<Integer, Integer>create()).apply(ParDo.of(new IntegerToStringFn()))
                .apply(TextIO.Write.to("gs://" + projectName + "/dataflow-files/evenNumbers"));

        p.run();
    }

    public class IntegerToTableRowFn extends DoFn<KV<Integer, Iterable<Integer>>, TableRow> {
        @Override
        public void processElement(DoFn.ProcessContext c) throws Exception {
            TableRow row = new TableRow();
            KV<Integer, Iterable<Integer>> item = (KV<Integer, Iterable<Integer>>) c.element();

            row.set("number", item.getKey());
            int teller = 0;
            Iterator iterator = item.getValue().iterator();
            while (iterator.hasNext()) {
                iterator.next();
                teller++;
            }
            row.set("occurences", teller);
            c.output(row);
        }
    }

    public class IntegerToStringFn extends DoFn<KV<Integer, Iterable<Integer>>, String> {
        @Override
        public void processElement(DoFn.ProcessContext c) throws Exception {
            TableRow row = new TableRow();
            KV<Integer, Iterable<Integer>> item = (KV<Integer, Iterable<Integer>>) c.element();
            String line = item.getKey().toString();
            int teller = 0;
            Iterator iterator = item.getValue().iterator();
            while (iterator.hasNext()) {
                iterator.next();
                teller++;
            }
            line += " - " + teller;
            c.output(line);
        }
    }


    public class IntegerToKVFn extends DoFn<Integer, KV<Integer, Integer>> {
        @Override
        public void processElement(DoFn.ProcessContext c) throws Exception {
            if (((Integer) c.element()) % 2 == 0) {
                c.sideOutput(evenNumbers, KV.of(c.element(), c.element()));
            } else {
                c.sideOutput(oddNumbers, KV.of(c.element(), c.element()));
            }
            c.output(KV.of(c.element(), c.element()));
        }
    }

    private TableSchema getBQSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();

        //LMD Project
        fields.add(rowDef("number", "INTEGER"));
        fields.add(rowDef("occurences", "INTEGER"));

        TableSchema schema = new TableSchema().setFields(fields);
        return schema;
    }

    public static TableFieldSchema rowDef(String name, String type) {
        return new TableFieldSchema().setName(name).setType(type);
    }

    public TableReference bqTableRef() {
        TableReference tableRef = new TableReference();
        tableRef.setProjectId(projectName);
        tableRef.setDatasetId("SampleDataSet");
        tableRef.setTableId("SampleTableId");
        return tableRef;
    }
}
