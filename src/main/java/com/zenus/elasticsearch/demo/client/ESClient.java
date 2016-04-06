package com.zenus.elasticsearch.demo.client;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ESClient {

    private TransportClient client;

    public ESClient() {
		final Settings settings = Settings.settingsBuilder()
			.build();
		
		client = TransportClient.builder().settings(settings).build();
		try {
			client.addTransportAddress(new InetSocketTransportAddress(
					InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 9300));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
    }

    public Client getClient() {
    	return client;
    }

    public void shutdown() {
		client.close();
		client = null;
    }

}
