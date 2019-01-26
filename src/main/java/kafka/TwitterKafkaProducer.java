package kafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class TwitterKafkaProducer {
    Logger logger = LoggerFactory.getLogger(TwitterKafkaProducer.class.getName());

    //Twitter Credentials
    String consumerKey= "Tbn37JTvndADiLgQgVgxof6mE";
    String consumerSecret="CB3lbhEx3ebsB3tphI4RUOweBZFqHyciTILW3KrAvINqServWX";
    String token="3011859313-7HOmuFDyYD2a03NXD3xdp1v7GwlozlnWDo2Qoi3 ";
    String secret="Zxq09BEgF7W8QepQJFY18Wvmax9JpBKxdWq9KvhMQbZd6";

    //Kafka Topic
    String kafkaTopic = "Twitter_Feed1";

    //HashTags to be followed
    List<String> hashTags= Lists.newArrayList("bitcoin", "usa", "politics", "sport");

    public TwitterKafkaProducer () {}

    public static void main(String[] args) {
        new TwitterKafkaProducer().run();
    }


    private void run() {
     logger.info("SetUp Stage");

        BlockingQueue<String> msgQueue = new LinkedBlockingDeque<String>(1000);

        //Create a Twitter Client
        Client twitterClient = createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        twitterClient.connect();

        //Create a Kafka Producer
         KafkaProducer<String,String> kafkaProducer = createKafkaProducer();

        //Add a ShutDown Hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
                    logger.info("Stopping Twitter Kafka Application");
                    logger.info("shutting down client from twitter");
                    twitterClient.stop();
                    logger.info("closing Kafka Producer");
                    kafkaProducer.close();
                    logger.info("Close");
                })
        );
       // new Thread( () -> {
            while(!twitterClient.isDone()) {
                String message = null;
                try
                {
                    message = msgQueue.poll(100, TimeUnit.MILLISECONDS);

                } catch(Exception e) {
                   logger.error("Exception During recieving tweets . Stopping Twitter Client ");
                    e.printStackTrace();
                    twitterClient.stop();
                }

                if(message!=null) {
                    logger.info("Recieved A Message From Twitter " );
                    logger.info("Recieved A Message From Twitter" + message);
                    System.out.println("Recieved Message" + message);

                    kafkaProducer.send(new ProducerRecord<>(kafkaTopic, null, message), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e!=null) {
                                logger.error("Error while Writing to Kafka " + e.getMessage());
                            }
                        }
                    });
                }
                else {
                    logger.info("Recieved Null Message");
                }
            }
      //  });


    }

    private KafkaProducer<String, String> createKafkaProducer() {


        //create Producer Properties

        Properties properties = createProperties();

        //Now create the Producer with above properties
        KafkaProducer<String,String> producer= new KafkaProducer<String, String>(properties);

        return producer;

    }

    private Properties createProperties() {
        String bootStrapServers = "0.0.0.0:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //Create a Safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG , "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        //High ThroughPut Producer

        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024)); //32Kb Batch

        return properties;
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(hashTags);

        //Twitter Secrets
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        //End of Connection Definition

        //Create Client

        ClientBuilder builder = new ClientBuilder()
                .name("TwitterClientProject1")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));


        Client hosebirdClient = builder.build();
        return hosebirdClient;


    }
}
