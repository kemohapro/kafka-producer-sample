package com.kemoha.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kemoha.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventProducer {
	
	@Autowired
	private KafkaTemplate<Integer, String> kafkaTemplate;
	
	@Autowired
	ObjectMapper objectMapper ;
	
	@Value("library-events")
	private String kafkaTopic;
	
	public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		
		System.out.println(value);
		
		ListenableFuture<SendResult<Integer,String>> listenableFuture =   kafkaTemplate.sendDefault(key, value);
		
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer,String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleOnSuccess(key , value , result);
				
			}

			@Override
			public void onFailure(Throwable ex) {
				
				handleOnFailure(key , value, ex) ;
			}
		}); 
	}
	
	public void sendLibraryEvent_usingProducer(LibraryEvent libraryEvent) throws JsonProcessingException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		
		System.out.println(value);
		
		
		ProducerRecord<Integer,String> producerRecord = producerRecord(key, value, kafkaTopic);
		
		ListenableFuture<SendResult<Integer,String>> listenableFuture =   kafkaTemplate.send(producerRecord);
		
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer,String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleOnSuccess(key , value , result);
				
			}

			@Override
			public void onFailure(Throwable ex) {
				
				handleOnFailure(key , value, ex) ;
			}
		}); 
	}
	
	
	private ProducerRecord<Integer,String> producerRecord(Integer key, String value, String kafkaTopic2) {
		
		List<Header> header = new ArrayList<Header>();
				
				
		header.add(new RecordHeader("event-source", "scanner".getBytes()));
		
		return new ProducerRecord<Integer,String>(kafkaTopic2,null,key,value,header);
	}

	public SendResult<Integer, String> postLibraryEvent_Synchronous(LibraryEvent libraryEvent) throws JsonProcessingException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		
		
		System.out.println(value);
		SendResult<Integer, String> sendResult =null;
		try {
			 sendResult =   kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			log.error("InterruptedException in sending the message and the exception is {} ",e.getMessage());
		} catch (ExecutionException e) {
			log.error("ExecutionException in sending the message and the exception is {} ",e.getMessage());
		} catch (TimeoutException e) {
			log.error("TimeoutException in sending the message and the exception is {} ",e.getMessage());
		}
		
		return sendResult;
	}

	protected void handleOnFailure(Integer key, String value, Throwable ex) {
		log.error("Error sending the message and the exception is {} ",ex.getMessage());
		try {
			throw ex;
		} catch (Throwable exception) {
			log.error("Erron on handleOnFailure() ",  exception.getMessage());
		}
		
		
	}

	protected void handleOnSuccess(Integer key, String value, SendResult<Integer, String> result) {
		log.info("Message sent successfully for key {}  value {} and partition {}" ,key , value,  result.getRecordMetadata().partition());
		
	}
}
