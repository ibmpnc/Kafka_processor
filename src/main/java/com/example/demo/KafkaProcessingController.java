package com.example.demo;

import java.io.StringReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.model.KafkaBankAccount;
import com.example.demo.model.KafkaXMLBankAccount;
import com.example.demo.serde.JsonDeserializer;
import com.example.demo.serde.JsonSerializer;
import com.example.demo.service.BankAccountService;
import com.example.demo.service.ConsumerCreator;

import lombok.extern.slf4j.Slf4j;

//@RestController
@Slf4j
public class KafkaProcessingController {

   
    private KafkaStreams bankAccountStream;
    @Autowired
	 BankAccountService bankAccountService;
    
    public static final long  WAIT_TIME_TO_CHECK_EDGE=120000;
    public static final long  ERROR_THRSHOLD=10;
    int succeedCount=0;
    int faildCount=0;
   
    
    @Value("${app.topic.example}")
    private String topic;
    
    @Value("${xmlapp.topic.example}")
    private String xmltopic;
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

  

    @RequestMapping("/stopStreams/")
    public void stopStreams() {
      
        bankAccountStream.close();
    }
    
    
    
    
    @RequestMapping("/startProcessingBankAccounts/")
    public void startProcessingBankAccounts() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "bankAccountStream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
       

        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, BankAccountSerde.class.getName());        
        
        
        final StreamsBuilder builder = new StreamsBuilder();

       
        
        KStream<String, KafkaBankAccount> source = builder.stream(topic);
        
        
        source.foreach((k,v)->{
        	
           
            log.info("Key="+ k + "   value="+ v);
        	bankAccountService.createAccount(v);
            //bankAccountService.createAccountCB(v);
            
        	
        });
       
        final Topology topology = builder.build();
        bankAccountStream = new KafkaStreams(topology, props);
        bankAccountStream.start();

    }
    
    
    boolean edgeServerUp=true;
    long timer=System.currentTimeMillis();
    boolean mongodbisHasRecs=true;
    
    
    @RequestMapping("/getProccessedCount/")
    public String getProccessedCount() {
    	return ("succeedCount="+this.succeedCount +"   faildCount="+this.faildCount  + "    totalProcessed ="+ (this.succeedCount +this.faildCount));
    	
    }
    
    @RequestMapping("/startProcessingxmlBankAccountsnew/")
    public void startProcessingxmlBankAccounts_new() {
    	
    	log.debug("startProcessingxmlBankAccounts_new....");
    	
    	Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "xmlbankAccountStream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass()); 
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        
        final StreamsBuilder builder = new StreamsBuilder();

       
        
        KStream<String, String> source = builder.stream(xmltopic);
        
      
       
        
        source.foreach((k,v)->{
        	
           // System.out.println("Key="+ k + "   value="+ v);
            log.info("Key="+ k + "   value="+ v);
        
            JAXBContext jaxbContext;
            try
            {
                jaxbContext = JAXBContext.newInstance(KafkaXMLBankAccount.class);              
             
                Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
             
                KafkaXMLBankAccount xmlaccount = (KafkaXMLBankAccount) jaxbUnmarshaller.unmarshal(new StringReader(v));
                 
                log.info(xmlaccount.toString());
                KafkaBankAccount account= new KafkaBankAccount(xmlaccount.getId(),xmlaccount.getBalance(),xmlaccount.getName(),xmlaccount.getAddress(),xmlaccount.getAccountType());
                
               // bankAccountService.createAccountCB(account);
                //bankAccountService.createAccount(account);
              
                if(edgeServerUp || (System.currentTimeMillis()-timer > WAIT_TIME_TO_CHECK_EDGE)) {
               	
              boolean result=  bankAccountService.createAccountRT(account);
              // 	boolean result=  bankAccountService.createAccountRTReactive(account);
                	
                	
                
                
                
                if(result && !edgeServerUp  && mongodbisHasRecs) {
                		bankAccountService.sendAllFromMongodb();
                		mongodbisHasRecs=false;
                	}
                	
                	if(!result ) {
                		edgeServerUp=false;
                		timer=System.currentTimeMillis();
                	}
                
                }else {
                	bankAccountService.createAccountMDBReactive(account);
                	if(!mongodbisHasRecs)
                		mongodbisHasRecs=true;
                }
                
             
            }
            catch (Exception e) 
            {
                e.printStackTrace();
            }
            
        	
        });
       
        final Topology topology = builder.build();
        bankAccountStream = new KafkaStreams(topology, props);
       
        bankAccountStream.start();

    }
    
    
    
    int  errorCount=0;
    int streamid=0;
    
    
    public void initializeKafkaStream() {
    	
    	log.debug("Start initializeKafkaStream....");
    	
    	Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, ("kafka_xml_bank_account_stream4"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass()); 
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        
        final StreamsBuilder builder = new StreamsBuilder();
        
         
        KStream<String, String> source = builder.stream(xmltopic);
        
      
       
        
        source.foreach((k,v)->{
        	
           // System.out.println("Key="+ k + "   value="+ v);
            log.info("Key="+ k + "   value="+ v);
        
            JAXBContext jaxbContext;
            try
            {
                jaxbContext = JAXBContext.newInstance(KafkaXMLBankAccount.class);              
             
                Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
             
                KafkaXMLBankAccount xmlaccount = (KafkaXMLBankAccount) jaxbUnmarshaller.unmarshal(new StringReader(v));
                 
                log.info(xmlaccount.toString());
                KafkaBankAccount account= new KafkaBankAccount(xmlaccount.getId(),xmlaccount.getBalance(),xmlaccount.getName(),xmlaccount.getAddress(),xmlaccount.getAccountType());
                 
               // bankAccountService.createAccountCB(account);
                //bankAccountService.createAccount(account);
              
               
               	
             // boolean result=  bankAccountService.createAccountRT(account);
              boolean result=  bankAccountService.createAccountMDB(account);
           //  boolean result=  bankAccountService.createAccountMDBReactive(account);
              //boolean result=  bankAccountService.createAccountRTReactive(account);
              
             // boolean result=true;
              
              if(account!=null)
              System.out.println("Account created "+ account);
            
              if(result) {
            	  errorCount=0;
            	  this.succeedCount++;
              }else {
            	  log.error("Error creating account "+account );
            	  errorCount++;
            	  this.faildCount++;
            	  
            	  log.error("ErrorCount= "+errorCount );
              }
                
              if(errorCount>this.ERROR_THRSHOLD){
            	 
            	  
            	  this.stopStream();
            	 
            	  
        	
             }
                
                
                
                
                
             
            }
            catch (Exception e) 
            {
                e.printStackTrace();
            }
            
        	
        });
       
        final Topology topology = builder.build();
        bankAccountStream = new KafkaStreams(topology, props);
       
        
        bankAccountStream.cleanUp();
        bankAccountStream.start();
        bankAccountStream.setUncaughtExceptionHandler((t, th) -> {
        	
        	log.error("UncaughtException in Kafka StreamThread  " + t.getName() + " exception = ", th.getMessage());
        	
        
            });
        
       
        
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
            	bankAccountStream.close();
            }
        }));

    }
    
    
    String streamStartSwitch="notStarted";
    
    public void startStream() {
    	
    	errorCount=0;
    	this.runConsumer();
    	//bankAccountStream.cleanUp();
    	//bankAccountStream.start();
    	
    	streamStartSwitch="active";
    	
		log.error("streamStartSwitch is on  after start state "+bankAccountStream.state() );
    }
    
    
 public void stopStream() {
	 
	 log.error("start stop stream "); 
	    
        bankAccountStream.close();
       
        log.error("streamStartSwitch is on  currnt state after clean up is "+bankAccountStream.state());
        bankAccountStream.cleanUp();
		log.error("streamStartSwitch is on  currnt state after close is "+bankAccountStream.state());
		
		
		streamStartSwitch="notactive";
    }
   
   @RequestMapping("/startapp/")
    public void start() {
	   
	   
	  
	   log.error("streamStartSwitch is on   "  +streamStartSwitch);
    	
    for(;;)	{
    	
    	
    	
    	if(streamStartSwitch.equalsIgnoreCase("on")) {
    		log.error("streamStartSwitch is on   " );
    		startStream();
    		
    	}else if(streamStartSwitch.equalsIgnoreCase("off")) {
    		log.error("streamStartSwitch is off  currnt state  before close is  "+bankAccountStream.state());
    		stopStream();
    		//break;
    	}
    	
    		
    	
    	
    	
    }	
  
    	
    }
    
    
    
    @RequestMapping("/turnOnStream/")
    public void turnOnStream() {
    	
    	log.debug("turnOnStream");
    	streamStartSwitch="on";
    }
    
    
    @RequestMapping("/turnOffStream/")
	public void turnOffStream() {
    	log.debug("turnOffStream");
	    	
	    	streamStartSwitch="off";
	 }
    
    
    
    @RequestMapping("/startProcessingxmlBankAccounts/")
    public void startProcessingxmlBankAccounts() {
    	log.debug("startProcessingxmlBankAccounts");
    	Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "xmlbankAccountStream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());   
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
       
        
        final StreamsBuilder builder = new StreamsBuilder();

       
        
        KStream<String, String> source = builder.stream(xmltopic);
        
      
        boolean edgeServerUp=true;
        
        source.foreach((k,v)->{
        	
        	log.info("Key="+ k + "   value="+ v);
           
        
            JAXBContext jaxbContext;
            try
            {
                jaxbContext = JAXBContext.newInstance(KafkaXMLBankAccount.class);              
             
                Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
             
                KafkaXMLBankAccount xmlaccount = (KafkaXMLBankAccount) jaxbUnmarshaller.unmarshal(new StringReader(v));
                 
                log.info(xmlaccount.toString());
                KafkaBankAccount account= new KafkaBankAccount(xmlaccount.getId(),xmlaccount.getBalance(),xmlaccount.getName(),xmlaccount.getAddress(),xmlaccount.getAccountType());
                 
               // bankAccountService.createAccountCB(account);
                //bankAccountService.createAccount(account);
              
               
                boolean result=	  bankAccountService.createAccountRT(account);
                	
                
              
              
              //System.out.println("Successul="+ result);
            }
            catch (Exception e) 
            {
                e.printStackTrace();
            }
            
        	
        });
       
        final Topology topology = builder.build();
        bankAccountStream = new KafkaStreams(topology, props);
        
        bankAccountStream.start();

    }
    
    
    
    static public final class BankAccountSerde extends WrapperSerde<KafkaBankAccount> {
        public BankAccountSerde() {
            super(new JsonSerializer<KafkaBankAccount>(), new JsonDeserializer<KafkaBankAccount>(KafkaBankAccount.class));
        }
    }
    
    
  
  boolean startFlag=true;
    
    @RequestMapping("/stop/")
    public void stop() {
    	startFlag=false;
    	
    }
    
   
    
    @RequestMapping("/start/")
    public void str() {
    	startFlag=true;
    	
    }
    
    
    
    
    @RequestMapping("/consumXML/")
    public void consumXML() {
    
    	
    	System.out.println("Start consumXML");
    	Properties props = new Properties();
    	props.put("bootstrap.servers", bootstrapServers);
    	props.put("group.id", "CountryCounter");
    	props.put("key.deserializer",
    	    "org.apache.kafka.common.serialization.StringDeserializer");
    	props.put("value.deserializer",
    	    "org.apache.kafka.common.serialization.StringDeserializer");

    	KafkaConsumer<String, String> consumer =
    		    new KafkaConsumer<String, String>(props);
    	
    	consumer.subscribe(Collections.singletonList("customerCountries"));

    	
    	
    	try {
    		
    		while (true) {
    			
    			
    			
    			
    			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
    			
    			System.out.println(records.count());
    			
    			
    			
    			for(ConsumerRecord<String, String> record : records) {
    			//	log.error("record.topic()=" +record.topic()+" record.partition()="+record.partition()
    			//	+" record.offset()="+record.offset()+" record.key()="+record.key()
    			//	+" record.value()="+record.value());
    				
    				
    				
    				
    			}
    		}
    		
    	}finally {
    		consumer.close();
    	}
    	
    	
    	
}

    
    
   // @RequestMapping("/consumXML/")
    public void consumXML1() {
    
    	
    	System.out.println("Start consumXML");
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList("xmltopic"));

    
    
    
    
    
    while (true) {
    	
    	System.out.println("Inside while loop......");
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
        System.out.println("records.count()="+records.count());
        records.count();
        
        for (ConsumerRecord<String, String> record : records)
            System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
    }
   

}
    boolean exitflag=false;
    long lastCommitedOffset=0;
	long lastOffset=0;
    
    @RequestMapping("/runConsumer/")
     void runConsumer() {
		Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
		

		int noMessageToFetch = 0;
		
		long startTime=System.currentTimeMillis();
		long endTime=System.currentTimeMillis();
		
		
		System.out.println("Start time" +startTime);
		
try {
		while (true) {
			
			
			
			if(this.startFlag ) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
			
			
			
			 

				 

			
			 
			 
			 
			
			 
			 
			 
			consumerRecords.forEach(record -> {
				System.out.println("Record Key " + record.key());
				System.out.println("Record offset " + record.offset()+" Record value " + record.value());
				System.out.println("Record partition " + record.partition());
				System.out.println("Record offset " + record.offset());
			
			
				lastOffset=record.offset();
			
				
				if(lastCommitedOffset==0)
					lastCommitedOffset=lastOffset-1;
			
				JAXBContext jaxbContext;
	            try
	            {
	                jaxbContext = JAXBContext.newInstance(KafkaXMLBankAccount.class);              
	             
	                Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
	             
	                KafkaXMLBankAccount xmlaccount = (KafkaXMLBankAccount) jaxbUnmarshaller.unmarshal(new StringReader(record.value()));
	                 
	                log.info(xmlaccount.toString());
	                KafkaBankAccount account= new KafkaBankAccount(xmlaccount.getId(),xmlaccount.getBalance(),xmlaccount.getName(),xmlaccount.getAddress(),xmlaccount.getAccountType());
	              
	              
	               
	               	
	             
	                boolean result=  bankAccountService.createAccountMDB(account);
	             
	                
	                
	                
	                
	                
	             
	            }
	            catch (Exception e) 
	            {
	                e.printStackTrace();
	            }
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			});
			
			
			
			
			
			
			
		}
		}
		
		
		
}finally {
		consumer.close();
}
	}
	
    
    
    public void whenCustomForEachIsCalled_ThenCorrectItemsAreReturned() {
        Stream<String> initialStream = Stream.of("cat", "dog", "elephant", "fox", "rabbit", "duck");
        List<String> result = new ArrayList<>();

        CustomForEach.forEach(initialStream, (elem, breaker) -> {
            if (elem.length() % 2 == 0) {
                breaker.stop();
            } else {
                result.add(elem);
            }
        });

        
    }

    
}
