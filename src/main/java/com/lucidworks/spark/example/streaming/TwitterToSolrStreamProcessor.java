package com.lucidworks.spark.example.streaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;

import com.lucidworks.spark.SolrSupport;
import com.lucidworks.spark.SparkApp;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.HashtagEntity;
import twitter4j.Status;

/**
 * Simple example of indexing tweets into Solr using Spark streaming; be sure to update the
 * twitter4j.properties file on the classpath with your Twitter API credentials.
 */
public class TwitterToSolrStreamProcessor extends SparkApp.StreamProcessor {

  public static Logger log = Logger.getLogger(TwitterToSolrStreamProcessor.class);

  public String getName() { return "twitter-to-solr"; }

  /**
   * Sends a stream of tweets to Solr.
   */
  public void plan(JavaStreamingContext jssc, CommandLine cli) throws Exception {
    String filtersArg = cli.getOptionValue("tweetFilters");
    String[] filters = (filtersArg != null) ? filtersArg.split(",") : new String[0];

    // start receiving a stream of tweets ...
    JavaReceiverInputDStream<Status> tweets =
      TwitterUtils.createStream(jssc, null, filters);
    
    final Random rand = new Random();
    // From http://www.enchantedlearning.com/wordlist/positivewords.shtml
    final List<String> positives = Arrays.asList("absolutely", "adorable", "accepted", "acclaimed", "accomplish", "accomplishment", "achievement", "action", "active", "admire", "adventure", "affirmative", "affluent", "agree", "agreeable", "amazing", "angelic", "appealing", "approve", "aptitude", "attractive", "awesome", "beaming", "beautiful", "believe", "beneficial", "bliss", "bountiful", "bounty", "brave", "bravo", "brilliant", "bubbly", "calm", "celebrated", "certain", "champ", "champion", "charming", "cheery", "choice", "classic", "classical", "clean", "commend", "composed", "congratulation", "constant", "cool", "courageous", "creative", "cute", "dazzling", "delight", "delightful", "distinguished", "divine", "earnest", "easy", "ecstatic", "effective", "effervescent", "efficient", "effortless", "electrifying", "elegant", "enchanting", "encouraging", "endorsed", "energetic", "energized", "engaging", "enthusiastic", "essential", "esteemed", "ethical", "excellent", "exciting", "exquisite", "fabulous", "fair", "familiar", "famous", "fantastic", "favorable", "fetching", "fine", "fitting", "flourishing", "fortunate", "free", "fresh", "friendly", "fun", "funny", "generous", "genius", "genuine", "giving", "glamorous", "glowing", "good", "gorgeous", "graceful", "great", "green", "grin", "growing", "handsome", "happy", "harmonious", "healing", "healthy", "hearty", "heavenly", "honest", "honorable", "honored", "hug", "idea", "ideal", "imaginative", "imagine", "impressive", "independent", "innovate", "innovative", "instant", "instantaneous", "instinctive", "intuitive", "intellectual", "intelligent", "inventive", "jovial", "joy", "jubilant", "keen", "kind", "knowing", "knowledgeable", "laugh", "legendary", "light", "learned", "lively", "lovely", "lucid", "lucky", "luminous", "marvelous", "masterful", "meaningful", "merit", "meritorious", "miraculous", "motivating", "moving", "natural", "nice", "novel", "now", "nurturing", "nutritious", "okay", "one", "one-hundred percent", "open", "optimistic", "paradise", "perfect", "phenomenal", "pleasurable", "plentiful", "pleasant", "poised", "polished", "popular", "positive", "powerful", "prepared", "pretty", "principled", "productive", "progress", "prominent", "protected", "proud", "quality", "quick", "quiet", "ready", "reassuring", "refined", "refreshing", "rejoice", "reliable", "remarkable", "resounding", "respected", "restored", "reward", "rewarding", "right", "robust", "safe", "satisfactory", "secure", "seemly", "simple", "skilled", "skillful", "smile", "soulful", "sparkling", "special", "spirited", "spiritual", "stirring", "stupendous", "stunning", "success", "successful", "sunny", "super", "superb", "supporting", "surprising", "terrific", "thorough", "thrilling", "thriving", "tops", "tranquil", "transforming", "transformative", "trusting", "truthful", "unreal", "unwavering", "up", "upbeat", "upright", "upstanding", "valued", "vibrant", "victorious", "victory", "vigorous", "virtuous", "vital", "vivacious", "wealthy", "welcome", "well", "whole", "wholesome", "willing", "wonderful", "wondrous", "worthy", "wow", "yes", "yummy", "zeal", "zealous");
    final List<String> negatives = Arrays.asList("abysmal", "adverse", "alarming", "angry", "annoy", "anxious", "apathy", "appalling", "atrocious", "awful", "bad", "banal", "barbed", "belligerent", "bemoan", "beneath", "boring", "broken", "callous", "can't", "clumsy", "coarse", "cold", "cold-hearted", "collapse", "confused", "contradictory", "contrary", "corrosive", "corrupt", "crazy", "creepy", "criminal", "cruel", "cry", "cutting", "dead", "decaying", "damage", "damaging", "dastardly", "deplorable", "depressed", "deprived", "deformed", "deny", "despicable", "detrimental", "dirty", "disease", "disgusting", "disheveled", "dishonest", "dishonorable", "dismal", "distress", "don't", "dreadful", "dreary", "enraged", "eroding", "evil", "fail", "faulty", "fear", "feeble", "fight", "filthy", "foul", "frighten", "frightful", "gawky", "ghastly", "grave", "greed", "grim", "grimace", "gross", "grotesque", "gruesome", "guilty", "haggard", "hard", "hard-hearted", "harmful", "hate", "hideous", "homely", "horrendous", "horrible", "hostile", "hurt", "hurtful", "icky", "ignore", "ignorant", "ill", "immature", "imperfect", "impossible", "inane", "inelegant", "infernal", "injure", "injurious", "insane", "insidious", "insipid", "jealous", "junky", "lose", "lousy", "lumpy", "malicious", "mean", "menacing", "messy", "misshapen", "missing", "misunderstood", "moan", "moldy", "monstrous", "naive", "nasty", "naughty", "negate", "negative", "never", "no", "nobody", "nondescript", "nonsense", "not", "noxious", "objectionable", "odious", "offensive", "old", "oppressive", "pain", "perturb", "pessimistic", "petty", "plain", "poisonous", "poor", "prejudice", "questionable", "quirky", "quit", "reject", "renege", "repellant", "reptilian", "repulsive", "repugnant", "revenge", "revolting", "rocky", "rotten", "rude", "ruthless", "sad", "savage", "scare", "scary", "scream", "severe", "shoddy", "shocking", "sick", "sickening", "sinister", "slimy", "smelly", "sobbing", "sorry", "spiteful", "sticky", "stinky", "stormy", "stressful", "stuck", "stupid", "substandard", "suspect", "suspicious", "tense", "terrible", "terrifying", "threatening", "ugly", "undermine", "unfair", "unfavorable", "unhappy", "unhealthy", "unjust", "unlucky", "unpleasant", "upset", "unsatisfactory", "unsightly", "untoward", "unwanted", "unwelcome", "unwholesome", "unwieldy", "unwise", "upset", "vice", "vicious", "vile", "villainous", "vindictive", "wary", "weary", "wicked", "woeful", "worthless", "wound", "yell", "yucky", "zero");

    // map incoming tweets into PipelineDocument objects for indexing in Solr
    JavaDStream<SolrInputDocument> docs = tweets.map(
      new Function<Status,SolrInputDocument>() {

        /**
         * Convert a twitter4j Status object into a SolrJ SolrInputDocument
         */
        public SolrInputDocument call(Status status) {

          if (log.isDebugEnabled())
            log.debug("Received tweet: " + status.getId() + ": " + status.getText().replaceAll("\\s+", " "));

          // simple mapping from primitives to dynamic Solr fields using reflection
          SolrInputDocument doc =
            SolrSupport.autoMapToSolrInputDoc("tweet-"+status.getId(), status, null);
          doc.setField("provider_s", "twitter");
          doc.setField("author_s", status.getUser().getScreenName());
          doc.setField("contributors_s", status.getContributors());
          doc.setField("created_at_s", status.getCreatedAt());
          doc.setField("text_s", status.getText());
          //doc.setField("favorite_count_s", status.getFavoriteCount());
          if (status.getGeoLocation() != null) {
            doc.setField("geo_location_latitude_f", status.getGeoLocation().getLatitude());
            doc.setField("geo_location_longitude_f", status.getGeoLocation().getLongitude());
          }
          doc.setField("id_i", status.getId());
          doc.setField("in_reply_to_screen_name_s", status.getInReplyToScreenName());
          doc.setField("in__reply_to_status_id_s", status.getInReplyToStatusId());
          //doc.setField("lang_s", status.getLang());
          if (status.getPlace() != null) {
            doc.setField("place_country_s", status.getPlace().getCountry());
            doc.setField("place_country_code_s", status.getPlace().getCountryCode());
            doc.setField("place_fullt_name_s", status.getPlace().getFullName());
            doc.setField("place_name_s", status.getPlace().getName());
            doc.setField("place_type_s", status.getPlace().getPlaceType());
            doc.setField("place_street_address_s", status.getPlace().getStreetAddress());
          }
          doc.setField("retweet_count_i", status.getRetweetCount());
          doc.setField("source_", status.getSource());
          //doc.setField("withheld_in_countries_s", status.getWithheldInCountries());
          doc.setField("is_favorited_b", status.isFavorited());
          doc.setField("is_possibly_sensitive_b", status.isPossiblySensitive());
          doc.setField("is_retweet_b", status.isRetweet());
          //doc.setField("is_retweeted_s", status.isRetweeted());
          doc.setField("is_retweeted_by_me_b", status.isRetweetedByMe());
          doc.setField("is_truncated_b", status.isTruncated());
          
          List<String> hashtags = new ArrayList<String>();
          for (HashtagEntity hashtag : status.getHashtagEntities()) {
            hashtags.add(hashtag.getText());
          }

          // Some "fake stats" to make it more interesting
          doc.setField("bytes_i", status.getText().length());
          int sentiment = 0;
          for (String word : status.getText().toLowerCase().split("\\W")) {
            if (positives.contains(word)) {
              sentiment += 1;
            }
            if (negatives.contains(word)) {
              sentiment -= 1;
            }
          }
          doc.setField("sentiment__score_i", sentiment);
          Calendar cal = Calendar.getInstance();
          cal.set(Calendar.YEAR, 1950 + rand.nextInt(50));
          doc.setField("_birthday_tdt", cal.getTime());

          return doc;
        }
      }
    );

    // analyze text to get an MLlib model

    // when ready, send the docs into a SolrCloud cluster
    SolrSupport.indexDStreamOfDocs(zkHost, collection, batchSize, docs);
  }

  public Option[] getOptions() {
    return new Option[]{
      OptionBuilder
              .withArgName("LIST")
              .hasArg()
              .isRequired(false)
              .withDescription("List of Twitter keywords to filter on, separated by commas")
              .create("tweetFilters")
    };
  }
}
