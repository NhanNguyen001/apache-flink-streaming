package com.nhannt22.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

@Slf4j
public class DateUtils {
    public static final ZoneId VIETNAM_TIMEZONE = ZoneId.of("Asia/Ho_Chi_Minh");
    public static final ZoneId DEFAULT_TIMEZONE = ZoneId.of(ZoneOffset.UTC.toString());
    public static LocalDateTime getLocalDateTimeNow(ZoneId zoneId) {
        return LocalDateTime.now(zoneId);
    }
    public static LocalDateTime plusMonth(LocalDateTime startDate, Integer month) {
        return startDate.plusMonths(month);
    }

    public static LocalDateTime plusWeek(LocalDateTime startDate, Integer week) {
        return startDate.plusWeeks(week);
    }

    public static LocalDateTime convertToLocalDateTime(String inputDatetime) {
        try {
            if (StringUtils.isEmpty(inputDatetime)) {
                return null;
            }
            // Parse the input string as an Instant
            Instant instant = Instant.parse(inputDatetime);

            // Convert the Instant to the target time zone
            return instant.atZone(DEFAULT_TIMEZONE).toLocalDateTime();
        } catch (DateTimeParseException ex) {
            // Handle the exception (e.g., log, throw custom exception, etc.)
            log.error("convertToLocalDateTime: {}", ex.getMessage());
            return null; // or throw a specific exception
        }
    }

    public static LocalDate getLocalDateNow(ZoneId zoneId) {
        return LocalDate.now(zoneId);
    }

    public static LocalDateTime toVietNameTimeZone(LocalDateTime localDateTime) {
        return localDateTime
                .atZone(DEFAULT_TIMEZONE)
                .withZoneSameInstant(VIETNAM_TIMEZONE)
                .toLocalDateTime();
    }
}
