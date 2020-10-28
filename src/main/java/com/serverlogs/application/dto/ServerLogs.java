package com.serverlogs.application.dto;

import lombok.Data;

@Data
public class ServerLogs {
	
	private String id;
	private String state;
	private Long timestamp;
	private String type;
	private String host;
	private Long duration;
	private boolean alert;

}
