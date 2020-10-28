package com.serverlogs.application;

import com.serverlogs.application.dto.ServerLogs;
import com.serverlogs.application.repository.ServerLogsRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ServerLogsApplication {

    public static void main(String[] args) throws IOException, SQLException {
        System.out.println("Enter File Path and Then Press Enter");
        Scanner sc = new Scanner(System.in);
        String fileName = sc.nextLine();
        Path path = Paths.get(fileName);

        Stream<String> lines = Files.lines(path);
        String data = lines.collect(Collectors.joining(",\n"));
        lines.close();
        data = "[" + data + "]";
        data = data.replaceAll(",,", ",");
        ObjectMapper objectMapper = new ObjectMapper();
        List<ServerLogs> serverLogsList = objectMapper.readValue(data, new TypeReference<List<ServerLogs>>() {
        });
		Map<String, ServerLogs> startLogs = new HashMap<>();
		Map<String, ServerLogs> finishedLogs = new HashMap<>();
        for(ServerLogs serverLogs : serverLogsList) {
        	if(serverLogs.getState().equals("STARTED")) {
        	    startLogs.put(serverLogs.getId(), serverLogs);
            } else {
        	    finishedLogs.put(serverLogs.getId(), serverLogs);
            }
		}

        ServerLogsRepository serverLogsRepository = new ServerLogsRepository();
        List<ServerLogs> finalServerLogs = new ArrayList<>();
        for(String id : startLogs.keySet()) {
            ServerLogs serverStartLogs = startLogs.get(id);
            ServerLogs serverFinishedLogs = finishedLogs.get(id);
            long timeElapsed = serverFinishedLogs.getTimestamp() - serverStartLogs.getTimestamp();
            serverStartLogs.setDuration(timeElapsed);
            serverStartLogs.setAlert(timeElapsed > 4);
            finalServerLogs.add(serverStartLogs);
        }
        serverLogsRepository.insertServerLogs(finalServerLogs);

    }

}
