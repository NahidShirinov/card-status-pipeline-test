package com.example.cardstatus;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootApplication
@EnableScheduling
public class CardStatusPipelineApplication {

    public static void main(String[] args) {
        SpringApplication.run(CardStatusPipelineApplication.class, args);
    }

    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder) {
        return builder
                .setConnectTimeout(Duration.ofSeconds(2))
                .setReadTimeout(Duration.ofSeconds(5))
                .build();
    }

    /**
     * One bounded pool shared by every batch request, instead of each
     * request spinning up its own - N concurrent batch calls used to mean
     * N independent pools (up to 20 threads each) with no shared limit.
     */
    @Bean(destroyMethod = "shutdown")
    public ExecutorService batchExecutor(@Value("${app.batch.executor-threads:20}") int threads) {
        return Executors.newFixedThreadPool(threads);
    }
}
