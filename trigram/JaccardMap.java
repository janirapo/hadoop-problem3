import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JaccardMap {

    private static final int ngram = 3;

    public static final Map<String, Integer> getGrammed(final String myString) {

        HashMap<String, Integer> gramMap = new HashMap<String, Integer>();

        for (int i = 0; i < (myString.length() - ngram + 1); i++) {
            String shingle = myString.substring(i, i + ngram);
            Integer old = gramMap.get(shingle);
            if (old != null) {
                gramMap.put(shingle, old + 1);
            } else {
                gramMap.put(shingle, 1);
            }
        }

        return Collections.unmodifiableMap(gramMap);
    }

    public static Double calculateJaccardSimilarity(final String s1, final String s2) {
        if (s1 == null || s1.length() == 0) {
            return 0d;
        }

        if (s2 == null || s2.length() == 0) {
            return 0d;
        }

        if (s1.equals(s2)) {
            return 1.0;
        }

        Map<String, Integer> gramProfile = getGrammed(s1);
        Map<String, Integer> gramProfile2 = getGrammed(s2);

        Set<String> union = new HashSet<String>();
        union.addAll(gramProfile.keySet());
        union.addAll(gramProfile2.keySet());

        int inter = gramProfile.keySet().size() + gramProfile2.keySet().size() - union.size();

        return 1.0 - (1.0 * inter / union.size());
    }

    /** @return an array of adjacent letter pairs contained in the input string */
    private static String[] letterPairs(String str) {
        int numPairs = str.length()-1;
        String[] pairs = new String[numPairs];
        for (int i=0; i<numPairs; i++) {
            pairs[i] = str.substring(i,i+2);
        }
        return pairs;
    }

    public static class PairGenerator extends Mapper<Object, Text, Text, DoubleWritable>{

        private static Vector<String> WordMap = new Vector<String>();
        private final static DoubleWritable one = new DoubleWritable(1);
        private String word = new String();
        private BufferedReader brReader;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            URI[] cacheFilesLocal = context.getCacheFiles();

            for (URI eachPath : cacheFilesLocal) {
                loadComparisonFile(eachPath, context);
            }
        }

        private void loadComparisonFile(URI filePath, Context context) throws IOException {
            String strLineRead = "";

            try {
                brReader = new BufferedReader(new FileReader(filePath.toString()));

                // Read each line, split and load to Vector
                while ((strLineRead = brReader.readLine()) != null) {
                    WordMap.add(strLineRead.trim().toLowerCase());
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                if (brReader != null) {
                    brReader.close();
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // loop each word in input 1 and create pair for all words in input 2
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word = itr.nextToken().toLowerCase();
                for(String word2 : WordMap){

                    Double similarity = calculateJaccardSimilarity(word, word2);

                    // which have Jaccard similarity not 0 and no larger than 0.15?
                    if ((similarity > 0.0) && (similarity <= 0.15)) {
                        DoubleWritable result = new DoubleWritable(similarity);
                        context.write(new Text(word+"\t"+word2), result);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.out.printf("Three parameters are required- <input1> <input2> <output dir>\n");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Jaccard  Map");

        Path output = new Path(args[2]);
        FileSystem fs = FileSystem.get(conf);

        // clear output if it existd
        if(fs.exists(output)){
            fs.delete(output, true);
        }

        job.setJarByClass(JaccardMap.class);
        job.addCacheFile(new Path(args[1]).toUri());

        job.setMapperClass(PairGenerator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
