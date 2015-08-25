package mr.func.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TopNFuncTest {
  MapDriver<Object, Text, Text, IntWritable> mapDriver;
  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

  @Before
  public void setup() {
    TopNFunc.Mapper mapper = new TopNFunc.Mapper();
    TopNFunc.Reducer reducer = new TopNFunc.Reducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
  }

  @Test
  public void testMapper() throws IOException{
    mapDriver.withInput(new LongWritable(), new Text(
      "[13/Jul/2015:07:57:03 +0200] GET /nmo/images/default/grid/grid3-special-col-bg__v1436564077871.gif 10.187.98.36 10.156.15.25 bsentnohead=837 qry= stat=200 sess=BCE7E9AE09201750653E802A4001090A thr=http-bio-9443-exec-8 usr=User{id=782444, username='N1408201'} scenId=- time=1"
    ));
    mapDriver.withOutput(new Text("N1408201"), new IntWritable(1));
    mapDriver.runTest();
  }

  @Test
  public void testReducer() throws IOException {
    List<IntWritable> values = new ArrayList<>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(2));
    reduceDriver.withInput(new Text("N1"), values);
    values.add(new IntWritable(4));
    reduceDriver.withInput(new Text("N2"), values);
    reduceDriver
      .withOutput(new Text("N2"), new IntWritable(7))
      .withOutput(new Text("N1"), new IntWritable(3));
    reduceDriver.runTest();
  }
}
