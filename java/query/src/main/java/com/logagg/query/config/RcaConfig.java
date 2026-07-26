package com.logagg.query.config;

import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.googleai.GoogleAiGeminiChatModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration for the RAG-based RCA Copilot.
 *
 * Wires the LangChain4j {@link ChatLanguageModel} as a Spring bean so that
 * {@link com.logagg.query.rca.RcaService} can be injected with it directly.
 *
 * The Gemini API key is resolved from the {@code GEMINI_API_KEY} environment
 * variable — it is never hardcoded in source or config files.
 */
@Configuration
public class RcaConfig {

    @Value("${rca.llm.api-key}")
    private String geminiApiKey;

    @Value("${rca.llm.model:gemini-1.5-flash}")
    private String modelName;

    @Value("${rca.llm.temperature:0.2}")
    private double temperature;

    @Value("${rca.llm.max-output-tokens:1024}")
    private int maxOutputTokens;

    /**
     * Creates a Gemini-backed {@link ChatLanguageModel} bean.
     *
     * Configuration choices:
     * - {@code gemini-1.5-flash} — fast, low-cost, large context (1M tokens).
     * - temperature 0.2 — keeps output factual and deterministic.
     * - maxOutputTokens 1024 — sufficient for structured JSON RCA responses.
     */
    @Bean
    public ChatLanguageModel chatLanguageModel() {
        return GoogleAiGeminiChatModel.builder()
                .apiKey(geminiApiKey)
                .modelName(modelName)
                .temperature(temperature)
                .maxOutputTokens(maxOutputTokens)
                .build();
    }
}
