package com.lucidworks.spark.example.streaming;

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
          //doc.setField("favorite_count_s", status.getFavoriteCount());
          if (status.getGeoLocation() != null) {
            doc.setField("geo_location_latitude_s", status.getGeoLocation().getLatitude());
            doc.setField("geo_location_longitude_s", status.getGeoLocation().getLongitude());
          }
          doc.setField("id_s", status.getId());
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
          doc.setField("retweet_count_s", status.getRetweetCount());
          doc.setField("source_", status.getSource());
          //doc.setField("withheld_in_countries_s", status.getWithheldInCountries());
          doc.setField("is_favorited_s", status.isFavorited());
          doc.setField("is_possibly_sensitive_s", status.isPossiblySensitive());
          doc.setField("is_retweet_s", status.isRetweet());
          //doc.setField("is_retweeted_s", status.isRetweeted());
          doc.setField("is_retweeted_by_me_s", status.isRetweetedByMe());
          doc.setField("is_truncated_s", status.isTruncated());
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
