package com.example.demo.service;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.example.demo.KafkaProcessingController.BankAccountSerde;
import com.example.demo.model.KafkaBankAccount;

@Component("scheduledService")
public class ScheduledService {

	
	@Autowired
	RestTemplate restTemplate	;
	
	
	
	 private KafkaStreams bankAccountStream;
	   
	 
	 @Autowired
		 BankAccountService bankAccountService;
	    
	    
	   
	    
	    @Value("${app.topic.example}")
	    private String topic;
	    
	    
	    
	    @Value("${spring.kafka.bootstrap-servers}")
	    private String bootstrapServers;
	
	
	//@Scheduled(fixedDelay = 60000)
    public void scheduleFixedDelayTask() {
        System.out.println("Fixed delay task - " + System.currentTimeMillis() / 1000);
       
        if(this.bankAccountService.isEdgeServiceUp())
             startProcessingBankAccounts();
    }
	
	
	public void startProcessingBankAccounts() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "bankAccountStream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
       

        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, BankAccountSerde.class.getName());        
        
        
        final StreamsBuilder builder = new StreamsBuilder();

       
        
        KStream<String, KafkaBankAccount> source = builder.stream(topic);
        
        
        source.foreach((k,v)->{
        	
            System.out.println("Key="+ k + "   value="+ v);
        	
        	try {
        		//bankAccountService.createAccount(v);
                //bankAccountService.createAccountCB(v);
				bankAccountService.createAccountRT(v);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
        	
        });
       
        final Topology topology = builder.build();
        bankAccountStream = new KafkaStreams(topology, props);
        bankAccountStream.start();

    }
    
}
