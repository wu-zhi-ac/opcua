package com.jellyleo.opcua.controller;

import com.jellyleo.opcua.client.IFMClientHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @package: com.jellyleo.opcua.controller
 * @Author: lenovo
 * @CreateTime: 2023-02-13 10:14
 * @Description:
 * @Version: 1.0
 */
@RestController
public class IFMController {

    @Autowired
    private IFMClientHandler ifmClientHandler;

    @RequestMapping("/start")
    public String start() throws Exception {
        ifmClientHandler.start();
        return "success";
    }
}
