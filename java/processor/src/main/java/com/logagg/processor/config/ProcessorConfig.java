package com.logagg.processor.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;

@Configuration
public class ProcessorConfig {

    @Value("${s3.endpoint:localhost:9000}")
    private String s3Endpoint;

    @Value("${s3.region:us-east-1}")
    private String s3Region;

    @Value("${s3.access-key:minioadmin}")
    private String s3AccessKey;

    @Value("${s3.secret-key:minioadmin}")
    private String s3SecretKey;

    @Value("${s3.use-ssl:false}")
    private boolean s3UseSsl;

    @Bean
    public S3Client s3Client() {
        String endpoint = s3Endpoint;
        if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
            endpoint = (s3UseSsl ? "https://" : "http://") + endpoint;
        }

        S3Configuration s3Config = S3Configuration.builder()
                .pathStyleAccessEnabled(true) // Required for MinIO
                .build();

        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(s3Region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(s3AccessKey, s3SecretKey)))
                .serviceConfiguration(s3Config)
                .build();
    }
}
