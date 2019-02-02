package com.spotonresponse.bigdata;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.spotonresponse.bigdata.utils.GetRssFeed;
import com.spotonresponse.bigdata.utils.XMLUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.*;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.*;


public class FetchRSS {

    private static final boolean debugging = false;


    private static final Logger logger = LogManager.getLogger(FetchRSS.class);
    private static DynamoDB myDynamoDB;
    private static AmazonDynamoDB client;
    // Used to hold all the entires in the database
    private static Map<String, String> DATABASE_ENTRIES;

    // Will be assigned from System.env()
    private static String DynamoDBTableName;
    private static String RssUrl;

    private static String amazon_endpoint;
    private static String amazon_region;


    private static String hostname;
    private static String programLocation;

    private static String ADAPTER_PREFIX;

    /*
     * This method will
     *  - Connect to URL and get RSS feed
     *  - Munge RSS XML into JSON
     *  - Pull out just the "items" in the feed
     *  - Create an MD5 hash - so we can compare if data has changed since last poll
     *  -
     */
    private static void processRss() throws IOException {
        URL url = new URL(RssUrl);
        JSONObject xmlJSONObj;
        if (debugging) {
            xmlJSONObj = new GetRssFeed().getFeed2(new File("/Users/edipko/Downloads/xcore-data.xml"));
            logger.debug(xmlJSONObj);
        } else {
            xmlJSONObj = new GetRssFeed().getFeed(url);
        }



        if (!debugging) {

            try {
                logger.debug("Parsing rss...channel... and items");
                JSONObject rss = (JSONObject) xmlJSONObj.get("rss");
                JSONObject channel = (JSONObject) rss.get("channel");
                JSONArray items = (JSONArray) channel.get("item");

                logger.debug("Setting up DynamoDB client");
                Table table = myDynamoDB.getTable(DynamoDBTableName);
                logger.debug("Connected to DynamoDB table");

                // Will be used to track items in the current RSS feed
                // To determine if they need to be removed
                List<String> rssItems = new ArrayList<String>();

                logger.debug("Looping though RSS items");
                // Loop through the RSS feed and determine if anything needs added or updated in the database
                Iterator itemIterator = items.iterator();
                while (itemIterator.hasNext()) {
                    JSONObject itemAll = (JSONObject) itemIterator.next();
                    if (itemAll.get("status").equals("Closed")) {

                  /*  JSONObject where = itemAll.getJSONObject("where");
                    JSONObject point = where.getJSONObject("Point");
                    String pos = point.getString("pos");
                    String[] loc = pos.split(" ");
                    String latitude = loc[1];
                    String longitude = loc[2];

                    //GeoDataManager geoDataManager = // Instantiate GeoDataManager
*/

                        // Make sure every key has a value
                        JSONObject item = XMLUtils.removeEmptyKeyValuePair(itemAll);
                        logger.trace("Item is: " + item.toString());

                        // Create an MD5 hash of the item and store it with the item
                        byte[] bytesOfItem = item.toString().getBytes(Charset.defaultCharset());
                        MessageDigest md = MessageDigest.getInstance("MD5");
                        byte[] digest = md.digest(bytesOfItem);
                        StringBuffer sb = new StringBuffer();
                        for (byte b : digest) {
                            sb.append(String.format("%02x", b & 0xff));
                        }
                        String md5Item = new String(sb.toString());
                        item.put("md5hash", md5Item);

                        //Get the values for the indexes (keys)
                        //  We need to remove the ADAPTER_PREFIX before comparing hashes
                        String md5hash = item.getString("md5hash");

                        String title = item.getString("title");

                        if (!debugging) {
                            // Determine if we already have the item
                            if (DATABASE_ENTRIES.containsKey(title)) {
                                // We have already seen this item, check to see if things have changed
                                String currentItemMd5hash = DATABASE_ENTRIES.get(title);
                                if (currentItemMd5hash.equals(md5hash)) {
                                    // Items are the same - nothing to do
                                    logger.trace("Hashes match - no update needed for: " + title);
                                } else {
                                    // We need to update the database with the new item
                                    logger.debug("Hashes do not match for title: " + title + " - Updating DB");
                                    logger.trace(" DBHash: " + currentItemMd5hash);
                                    logger.trace("RSSHash: " + md5hash);
                                    if (updateDatabase(item, table)) {
                                        logger.debug("Updating item in DB cache");
                                        DATABASE_ENTRIES.remove(title);
                                        DATABASE_ENTRIES.put(title, md5hash);
                                    }
                                }
                            } else {
                                // We do not have this yet - add it to the database
                                logger.debug(title + " not found in array - Add to DB");
                                if (updateDatabase(item, table)) {
                                    logger.debug("Adding item to local DB cache");
                                    DATABASE_ENTRIES.put(title, ADAPTER_PREFIX + md5hash);
                                }
                            }
                        }

                        // Add the item to the List
                        rssItems.add(title);
                    }
                }


                // Now check to see if anything need removed from the database
                logger.debug("Checking if things need removed from database");
                logger.trace("The feed has " + rssItems.size() + " items");
                logger.trace("The database has " + DATABASE_ENTRIES.size() + " items");


                // Loop and determine what need to be deleted
                // store what needs delete in this HashMap
                Map<String, String> deleteHashMap = new HashMap<String, String>();
                Iterator dbItemIterator = DATABASE_ENTRIES.entrySet().iterator();
                while (dbItemIterator.hasNext()) {
                    Map.Entry dbPair = (Map.Entry) dbItemIterator.next();
                    String dbKey = dbPair.getKey().toString();
                    String dbIndex = dbPair.getValue().toString();
                    // Store what needs delete
                    if (!rssItems.contains(dbKey)) {
                        if (deleteEntry(dbPair, table)) {
                            deleteHashMap.put(dbKey, dbIndex);
                        }

                    }
                }

                // Loop through what needs deleted and remove it from the HashMap (Local DB cache)
                Iterator deleteIterator = deleteHashMap.entrySet().iterator();
                while (deleteIterator.hasNext()) {
                    // Delete from the cache.
                    Map.Entry dbPair = (Map.Entry) deleteIterator.next();
                    DATABASE_ENTRIES.remove(dbPair.getKey());
                }

            } catch (Exception ex) {
                ex.printStackTrace();
            }

        }


    }


    private static boolean deleteEntry(Map.Entry key, Table table) {

        logger.debug("Removing entry from database: " + key.getKey() + " | " + key.getValue());

        try {
            DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                    .withPrimaryKey(new PrimaryKey("title", key.getKey(), "md5hash", ADAPTER_PREFIX + key.getValue()));
            table.deleteItem(deleteItemSpec);
            logger.trace("Remove complete");
        } catch (Exception ex) {
            logger.error("Unable to delete item: " + key.getKey());
            logger.error(ex.getMessage());
            return false;
        }


        return true;

    }

    private static boolean updateDatabase(JSONObject item, Table table) {

        //Get the values for the indexes (keys)
        String md5hash = item.getString("md5hash");
        String title = item.getString("title");

        item.put("Data Source URL", RssUrl);
        item.put("EndPoint", amazon_endpoint);
        item.put("Adapter Name", "rsspoller");
        item.put("Adapter Host", hostname);
        item.put("Adapter Type", "script");
        item.put("Adapter Location", programLocation);

        logger.debug("Title: " + title + " | MD5hash: " + md5hash);
        try {
            table.putItem(new Item().withPrimaryKey("md5hash", ADAPTER_PREFIX + md5hash, "title", title).withJSON("item", item.toString()));
            logger.debug("PutItem succeeded: " + title + " | MD5hash: " + md5hash);

        } catch (Exception e) {
            logger.error("Unable to add event: " + title + " | MD5hash: " + md5hash);
            logger.error(e.getMessage());
            return false;
        }

        return true;
    }


    public static Map<String, String> getDBKeys() {

        logger.debug("Loading ALL keys from DB...");
        Map<String, String> currentItems = new HashMap<String, String>();
        ScanResult result = null;


        do {
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(DynamoDBTableName);

            result = client.scan(scanRequest);
            for (Map<String, AttributeValue> item : result.getItems()) {
                String hash = item.get("md5hash").getS();
                // TODO: Would really like to filter this in the query - but was not able to
                // figure it out quickly...
                if (hash.startsWith(ADAPTER_PREFIX)) {
                    // Create the current items list, and remove the ADAPTER PREFIX from the hash
                    currentItems.put(item.get("title").getS(), hash.replace(ADAPTER_PREFIX, ""));
                }
            }

            logger.debug("...Done");
        } while (result.getLastEvaluatedKey() != null);

        return currentItems;
    }



    public static void main(String[] args) throws IOException {

        // Set the hostname
        hostname = InetAddress.getLocalHost().getHostName();
        try {
            programLocation = new File(FetchRSS.class.getProtectionDomain().getCodeSource().getLocation()
                    .toURI()).getPath();
        } catch (URISyntaxException e) {
            logger.error("Unable to get patch of java JAR file");
        }

        if (debugging) {
            // Fetch the RSS Feed and process it
            RssUrl = "https://walmart.alertlink.com/rss/stores.rss";

            FetchRSS.processRss();
        } else {
            while (true) {

                // Get some envirnoment variables
                long seconds_between_runs = Long.parseLong(System.getenv("seconds_between_runs"));
                long seconds_between_dbcache_refresh = Long.parseLong(System.getenv("seconds_between_dbcache_refresh"));
                amazon_endpoint = System.getenv("amazon_endpoint");
                amazon_region = System.getenv("amazon_region");
                String aws_access_key_id = System.getenv("AWS_ACCESS_KEY_ID");
                String aws_secret_access_key = System.getenv("AWS_SECRET_ACCESS_KEY");
                ADAPTER_PREFIX = System.getenv("ADAPTER_PREFIX");

                DynamoDBTableName = System.getenv("db_table_name");
                RssUrl = System.getenv("rss_url");

                logger.info("FetchRSS processor starting up...");
                logger.info("Timers:  Seconds between run: " + seconds_between_runs);
                logger.info("         Seconds between db cache refresh: " + seconds_between_dbcache_refresh);
                logger.info("URLs:    Amazon Endpoint: " + amazon_endpoint + " Region: " + amazon_region);
                logger.info("         RssFeed URL: " + RssUrl);
                logger.info("Filter:  Hash Prefix: " + ADAPTER_PREFIX);
                logger.info("Database Table Name: " + DynamoDBTableName);


                int num_runs = 0;

                // Setup database connection
                BasicAWSCredentials awsCreds = new BasicAWSCredentials(aws_access_key_id, aws_secret_access_key);

                client = AmazonDynamoDBClientBuilder.standard()
                        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(amazon_endpoint, amazon_region))
                        .build();
                myDynamoDB = new DynamoDB(client);

                // Initial startup - grab what is in the database
                if (num_runs == 0) {
                    logger.debug("Updating local DB cache with online DB contents...");
                    DATABASE_ENTRIES = getDBKeys();
                }

                // Fetch the RSS Feed and process it
                FetchRSS.processRss();

                // Don't hold a connection open to the database
                myDynamoDB.shutdown();

                // We will refresh the local DB cache from the online database
                // at a regular configured interval.
                if (num_runs * seconds_between_runs > seconds_between_dbcache_refresh) {
                    logger.debug("DB Cache refresh reached, will refresh the database on the next run");
                    num_runs = 0;
                } else {
                    num_runs++;
                }


                logger.debug("Sleeping for " + seconds_between_runs + " seconds.");
                try {
                    Thread.sleep(seconds_between_runs * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }

    }
}
