package hadoop.mapreduce.tp1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

public class StoreVisitsPerUser {
    public static class VisitMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            if (tokens.length == 4) {
                String user = tokens[2];  // Numéro du client
                String store = tokens[3]; // Magasin visité
                context.write(new Text(user), new Text(store)); // Écrire la clé (client) et la valeur (magasin)
            }
        }
    }

    public static class UniqueStoreReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> uniqueStores = new HashSet<>();
            // Collecter les magasins uniques visités par le client
            for (Text val : values) {
                uniqueStores.add(val.toString());
            }

            // Préparer la sortie : numéro du client, liste des magasins visités, et le nombre total de magasins
            String storesList = String.join(",", uniqueStores);
            String output = storesList + " | Total: " + uniqueStores.size();

            context.write(key, new Text(output));  // Écrire le résultat final
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Store Visits Per User");
        job.setJarByClass(StoreVisitsPerUser.class);
        job.setMapperClass(VisitMapper.class);
        job.setReducerClass(UniqueStoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
