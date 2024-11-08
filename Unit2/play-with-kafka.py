# -*- coding: utf-8 -*-
"""
Created on Thu Oct 31 16:58:31 2024

@author: cassi
"""
import org.apache.kafka.streams.StreamsConfig;

final Properties streamsConfiguration = new Properties();
streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "Kafka-music-charts");
streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
