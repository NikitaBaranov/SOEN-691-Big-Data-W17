package ca.concordia.encs.n_baran;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class WhoToFollow {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf1 = new Configuration();
        Job job = Job.getInstance(conf1, "Who To Follow Index");
        job.setJarByClass(WhoToFollow.class);

        job.setMapperClass(WhoToFollowIndexMapper.class);
        job.setReducerClass(WhoToFollowIndexReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        String intermediateOutputPath = "output_"+System.currentTimeMillis();
        FileOutputFormat.setOutputPath(job, new Path(intermediateOutputPath));

        job.waitForCompletion(true);


        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Who To Follow Similarity");
        job2.setJarByClass(WhoToFollow.class);

        job2.setMapperClass(AllPairsMapper.class);
        job2.setReducerClass(WhoToFollowIndexReducer.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(intermediateOutputPath));
        FileOutputFormat.setOutputPath(job2, new Path("output_"+System.currentTimeMillis()));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }


    /**********************/
    /**      Mappers     **/
    /**********************/

    //Mapper fot Who To Follow Indexing
    public static class WhoToFollowIndexMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        // Emits inverted list.
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(values.toString());
            IntWritable user = new IntWritable();
            IntWritable follower = new IntWritable();

            follower.set(Integer.parseInt(st.nextToken()));

            while (st.hasMoreTokens()) {
                user.set(Integer.parseInt(st.nextToken()));
                context.write(user, follower);
            }
        }
    }

    //Mapper for People you may know
    public static class AllPairsMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        // Emits (a,b) *and* (b,a) any time a friend common to a and b is found.
        // Emits (x,-f) for all freands that already know each other
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            // Key is ignored as it only stores the offset of the line in the text file
            StringTokenizer st = new StringTokenizer(values.toString());
            // seenFriends will store the friends we've already seen as we walk through the list of friends
            ArrayList<Integer> seenFriends = new ArrayList<>();
            // friend1 and friend2 will be the elements in the emitted pairs.
            IntWritable friend1 = new IntWritable();
            IntWritable friend2 = new IntWritable();
            IntWritable friendMain = new IntWritable();

            friendMain.set(Integer.parseInt(st.nextToken()));


            while (st.hasMoreTokens()) {
                // For every friend Fi found in the values,
                // we emit (Fi,Fj) and (Fj,Fi) for every Fj in the
                // friends we have seen before. You can convince yourself
                // that this will emit all (Fi,Fj) pairs for i!=j.
                friend1.set(Integer.parseInt(st.nextToken()));
                for (Integer seenFriend : seenFriends) {
                    friend2.set(seenFriend);
                    context.write(friend1, friend2);
                    context.write(friend2, friend1);
                }
                context.write(friendMain, new IntWritable(friend1.get() * -1));
                seenFriends.add(friend1.get());
            }
        }
    }

    /**********************/
    /**      Reducer     **/
    /**********************/

    // Reducer fo Whom to follow Indexing
    public static class WhoToFollowIndexReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer(""); // Using a StringBuffer is more efficient than concatenating strings
            while (values.iterator().hasNext()) {
                sb.append(values.iterator().next().get());
                sb.append(" ");
            }
            Text result = new Text(sb.toString());
            context.write(key, result);
        }
    }

    // Reducer for People you may know
    public static class CountReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        // The reduce method
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // user stores the id of the user for which we are searching for recommendations
            IntWritable user = key;
            // recommendations will store all the recommendations for user 'user'
            ArrayList<Recommendation> recommendations = new ArrayList<>();

            // Builds the recommendation array
            while (values.iterator().hasNext()) {
                int userWithCommonFriend = values.iterator().next().get();
                Recommendation p = Recommendation.find(userWithCommonFriend, recommendations);
                if (p == null)
                    // no recommendation exists for user 'userWithCommonFriend'. Let's create one.
                    recommendations.add(new Recommendation(userWithCommonFriend));
                else
                    // there is already a recommendation for user 'userWithCommonFriend'. Let;s
                    // increment the number of friends in common.
                    p.addCommonFriend(userWithCommonFriend);
            }
            // Sorts the recommendation array by number of common friends
            // See javadoc on Comparator at https://docs.oracle.com/javase/8/docs/api/java/util/Comparator.html
            recommendations.sort(new Comparator<Recommendation>() {
                @Override
                public int compare(Recommendation t, Recommendation t1) {
                    return -Integer.compare(t.getNCommonFriends(), t1.getNCommonFriends());
                }
            });
            // Builds the output string that will be emitted
            StringBuffer sb = new StringBuffer(""); // Using a StringBuffer is more efficient than concatenating strings
            for (int i = 0; i < recommendations.size() && i < 10; i++) {
                Recommendation p = recommendations.get(i);
                sb.append(p.toString());
            }
            Text result = new Text(sb.toString());
            context.write(user, result);
        }

        // A private class to describe a recommendation.
        // A recommendation has a friend id and a number of friends in common.
        private static class Recommendation {

            // Attributes
            private int friendId;
            private int nCommonFriends;
            private boolean isAlreadyFriends;

            // Constructor
            public Recommendation(int newFriendId) {
                this.friendId = Math.abs(newFriendId);
                // A recommendation must have at least 1 common friend
                this.nCommonFriends = 1;
                this.isAlreadyFriends = false;
                // If users are already friends
                if (newFriendId < 0) {
                    this.isAlreadyFriends = true;
                };
            }

            // Finds a representation in an array
            public static Recommendation find(int friendId, ArrayList<Recommendation> recommendations) {
                for (Recommendation p : recommendations)
                    if (p.getFriendId() == Math.abs(friendId))
                        return p;
                // Recommendation was not found!
                return null;
            }

            // Getters
            public int getFriendId() {
                return friendId;
            }

            public int getNCommonFriends() {
                return nCommonFriends;
            }

            // Other methods
            // Increments the number of common friends
            public void addCommonFriend(int userWithCommonFriend) {
                if (userWithCommonFriend < 0) this.isAlreadyFriends = true;
                nCommonFriends++;
            }

            // String representation used in the reduce output
            public String toString() {
//                if(!isAlreadyFriends) {
                    return friendId + "(" + nCommonFriends + ") ";
//                } else return "";
            }
        }
    }
}
