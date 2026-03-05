package com.logagg.common.model;

public enum LogLevel {
    DEBUG, INFO, WARN, ERROR;

    public static LogLevel fromString(String s) {
        if (s == null || s.isBlank()) return INFO;
        try {
            return valueOf(s.toUpperCase());
        } catch (IllegalArgumentException e) {
            return INFO;
        }
    }
}
