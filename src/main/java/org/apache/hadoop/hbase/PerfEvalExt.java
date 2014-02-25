package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.PerformanceEvaluation.Status;
import org.apache.hadoop.hbase.PerformanceEvaluation.Test;
import org.apache.hadoop.hbase.PerformanceEvaluation.TestOptions;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Has to be in this package because it's accessing protected items on PerformanceEvalution.
 * Can only be run in nomapreduce mode because PerformanceEvaluation.java breaks our additions
 * be creating new PerformanceEvaluations that don't have these tests.
 *
 * @author kim
 */
public class PerfEvalExt extends Configured implements Tool {

    public static final byte[] TABLE_NAME = Bytes.toBytes("TestTable");
    public static final byte[] FAMILY_NAME = Bytes.toBytes("info");
    public static final byte[] QUALIFIER_NAME = Bytes.toBytes("data");

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new PerfEvalExt(), args);
        System.exit(ret);
    }

    private static byte[] generateFixedValue(final int i) {
        String formattedValue = Integer.toString(i);
        return Bytes.toBytes(formattedValue);
    }

    /*
     * Format passed integer.
     *
     * @param number
     *
     * @return Returns zero-prefixed 10-byte wide decimal version of passed
     * number (Does absolute in case number is negative).
     */
    public static byte[] format(final int number) {
        byte[] b = new byte[10];
        int d = Math.abs(number);
        for (int i = b.length - 1; i >= 0; i--) {
            b[i] = (byte) ((d % 10) + '0');
            d /= 10;
        }
        return b;
    }

    public int run(String[] arg0) throws Exception {
        Configuration c = HBaseConfiguration.create(getConf());

        PerformanceEvaluation pe = new PerformanceEvaluation(c);
        pe.addCommandDescriptor(FloatWriteTest.class,
                "floatWrite", "Run float write test");
        pe.addCommandDescriptor(BatchWriteTest.class,
                "batchWrite", "Run batch write test");
        pe.addCommandDescriptor(VerificationReadTest.class, "verificationRead",
                "Run verificationRead read test");
        pe.addCommandDescriptor(VerificationWriteTest.class,
                "verificationWrite", "Run verificationWrite write test");
        return pe.run(arg0);
    }

    static class BatchWriteTest extends Test {
        private static final int BATCH_SIZE = 5000;
        List<Put> puts = new ArrayList<Put>();

        BatchWriteTest(Configuration conf, TestOptions options,
                       Status status) {
            super(conf, options, status);
        }

        @Override
        void testRow(final int i) throws IOException {
            String key = String.format("%d-%d", rand.nextInt(9), System.currentTimeMillis());
            Put put = new Put(Bytes.toBytes(key));
            byte[] value = Bytes.toBytes(rand.nextFloat());
            put.add(FAMILY_NAME, QUALIFIER_NAME, value);
            put.setWriteToWAL(writeToWAL);
            puts.add(put);
            if (puts.size() > BATCH_SIZE) {
                table.put(puts);
                puts.clear();
            }
        }

    }

    static class FloatWriteTest extends Test {
        FloatWriteTest(Configuration conf, TestOptions options,
                       Status status) {
            super(conf, options, status);
        }

        @Override
        void testRow(final int i) throws IOException {
            String key = String.format("%d-%d", rand.nextInt(9), System.currentTimeMillis());
            Put put = new Put(Bytes.toBytes(key));
            byte[] value = Bytes.toBytes(rand.nextFloat());
            put.add(FAMILY_NAME, QUALIFIER_NAME, value);
            put.setWriteToWAL(writeToWAL);
            table.put(put);
        }

    }

    static class VerificationReadTest extends Test {
        VerificationReadTest(Configuration conf, TestOptions options,
                             Status status) {
            super(conf, options, status);
        }

        @Override
        void testRow(final int i) throws IOException {
            Get get = new Get(format(i));
            get.addColumn(FAMILY_NAME, QUALIFIER_NAME);
            Result res = table.get(get);
            byte[] resultBytes = res.getValue(FAMILY_NAME, QUALIFIER_NAME);
            boolean eq = Bytes.equals(resultBytes, generateFixedValue(i));
            if (!eq) {
                System.err.println("Row " + Bytes.toString(format(i)) + " not expected");
            }
        }
    }

    static class VerificationWriteTest extends Test {
        VerificationWriteTest(Configuration conf, TestOptions options,
                              Status status) {
            super(conf, options, status);
        }

        @Override
        void testRow(final int i) throws IOException {
            Put put = new Put(format(i));
            byte[] value = generateFixedValue(i);
            put.add(FAMILY_NAME, QUALIFIER_NAME, value);
            put.setWriteToWAL(writeToWAL);
            table.put(put);
        }

    }
}
