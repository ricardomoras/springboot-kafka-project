package org.kafka.consumer.database;

import org.kafka.consumer.database.entity.WikimediaData;
import org.kafka.consumer.database.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafKaDataBaseConsumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafKaDataBaseConsumer.class);
	
	private WikimediaDataRepository dataRepository;
	
	
	
	public KafKaDataBaseConsumer(WikimediaDataRepository dataRepository) {
		this.dataRepository = dataRepository;
	}


	@KafkaListener(topics = "wikimedia_recentchange", groupId = "myGroup")
	public void consume(String eventMessage) {
		LOGGER.info(String.format("Event message received -> %s",eventMessage));
		
		WikimediaData wikimediaData = new WikimediaData();
		wikimediaData.setWikiEventData(eventMessage);
		
		dataRepository.save(wikimediaData);
	}
}
