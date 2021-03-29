package com.example.demo;

import java.io.StringReader;
import java.time.Duration;
import java.util.Properties;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Serdes;
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
import com.example.demo.service.BankAccountService;
import com.example.demo.service.ConsumerCreator;
import com.example.demo.service.IKafkaConstants;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j

public class KafkaStreamingController {

	
	 private KafkaStreams bankAccountStream;
	    @Autowired
		 BankAccountService bankAccountService;
	    
	 
		long streamProcessedCount=0;
		boolean startStreamFlag=true;
		
		
		    
		    @Value("${xmlapp.topic.example}")
		    private String xmltopic;
		    
		    @Value("${spring.kafka.bootstrap-servers}")
		    private String bootstrapServers;
	
	
	
	
	 @RequestMapping("/startStreeming/")
	    public void startStreeming() {
	    	
	    	log.debug("Start initializeKafkaStream....");
	    	startStreamFlag=true;
	    	Properties props = new Properties();
	        props.put(StreamsConfig.APPLICATION_ID_CONFIG, ("kafka_xml_bank_account_stream4"));
	        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass()); 
	        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	        
	        streamProcessedCount=0;
	        final StreamsBuilder builder = new StreamsBuilder();
	        
	         
	        KStream<String, String> source = builder.stream(xmltopic);
	         source.foreach((k,v)->{
	        	
	          
	           
	        
	            JAXBContext jaxbContext;
	            try
	            {
	                jaxbContext = JAXBContext.newInstance(KafkaXMLBankAccount.class);              
	             
	                Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
	             
	                KafkaXMLBankAccount xmlaccount = (KafkaXMLBankAccount) jaxbUnmarshaller.unmarshal(new StringReader(v));
	                 
	                log.debug(xmlaccount.toString());
	                KafkaBankAccount account= new KafkaBankAccount(xmlaccount.getId(),xmlaccount.getBalance(),xmlaccount.getName(),xmlaccount.getAddress(),xmlaccount.getAccountType());
	                bankAccountService.createAccountMDB(account);
	                streamProcessedCount++;
	                 
	                
	                if(!this.startStreamFlag){
	               	 
	              	  
	                	bankAccountStream.close();
	                	bankAccountStream.cleanUp();
	              	 
	              	  
	          	
	               }
	                
	                Thread.sleep(20);
	             
	            }
	            catch (Exception e) 
	            {
	                e.printStackTrace();
	            }
	            
	        	
	        });
	       
	        final Topology topology = builder.build();
	        bankAccountStream = new KafkaStreams(topology, props);
	       
	        
	      ;
	        bankAccountStream.start();
	       // bankAccountStream.setUncaughtExceptionHandler((t, th) -> {
	        	
	       // 	log.error("UncaughtException in Kafka StreamThread  " + t.getName() + " exception = ", th.getMessage());
	        	
	        
	       //     });
	        
	       
	        
	        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
	            @Override
	            public void run() {
	            	bankAccountStream.close();
	            }
	        }));

	    }
	    

	    
	    @RequestMapping("/stopsteaming/")
	    public void stopStream() {
	    	log.debug("stop Streaming ");
	    	log.debug("Stream Porcessed Count="+ this.streamProcessedCount);
	    	this.startStreamFlag=false;
	    	
	    }
	    
}
