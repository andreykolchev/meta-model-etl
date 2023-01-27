package com.example.controller;

import com.example.model.dto.metamodel.AttributesRq;
import com.example.service.DataProcessingService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpSession;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/data-processing")
public class DataProcessingController {

    final
    DataProcessingService dataProcessingService;

    public DataProcessingController(DataProcessingService dataProcessingService) {
        this.dataProcessingService = dataProcessingService;
    }

    @PostMapping("/buildDataSet")
    public List<Map<String, Object>> buildDataSet(@RequestBody AttributesRq rq, HttpSession session) {
        String sessionId = session.getId();
        return dataProcessingService.buildDataSet(sessionId, rq.getAttributes());
    }

}
