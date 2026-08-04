/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.utils;

import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

import static java.time.temporal.ChronoField.NANO_OF_SECOND;

/**
 * We replace "." with "_" and replace ":" in date format with "-" so that
 * TablePath.detectInvalidName validation will accept the partition name.
 */
public class PartitionNameConverters {

    private PartitionNameConverters() {}

    public static String hexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xFF & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    public static String reformatFloat(Float value) {
        if (value.isNaN()) {
            return "NaN";
        } else if (value.isInfinite()) {
            return (value > 0) ? "Inf" : "-Inf";
        } else {
            return String.valueOf(value).replace(".", "_");
        }
    }

    public static String reformatDouble(Double value) {
        if (value.isNaN()) {
            return "NaN";
        } else if (value.isInfinite()) {
            return (value > 0) ? "Inf" : "-Inf";
        } else {
            return String.valueOf(value).replace(".", "_");
        }
    }

    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);

    public static String dayToString(int days) {
        OffsetDateTime day = EPOCH.plusDays(days);
        return String.format(
                Locale.ROOT,
                "%04d-%02d-%02d",
                day.getYear(),
                day.getMonth().getValue(),
                day.getDayOfMonth());
    }

    public static String milliToString(int milli) {
        int millisPerSecond = 1000;
        int millisPerMinute = 60 * millisPerSecond;
        int millisPerHour = 60 * millisPerMinute;

        int hour = Math.floorDiv(milli, millisPerHour);
        int min = Math.floorDiv(Math.floorMod(milli, millisPerHour), millisPerMinute);
        int seconds =
                Math.floorDiv(
                        Math.floorMod(Math.floorMod(milli, millisPerHour), millisPerMinute),
                        millisPerSecond);

        return String.format(
                Locale.ROOT,
                "%02d-%02d-%02d_%03d",
                hour,
                min,
                seconds,
                Math.floorMod(milli, millisPerSecond));
    }

    private static final DateTimeFormatter TimestampFormatter =
            new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-[MM]-[dd]")
                    .optionalStart()
                    .appendPattern("-[HH]-[mm]-[ss]_")
                    .appendFraction(NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .toFormatter();

    /** always add nanoseconds whether TimestampNTZ and TimestampLTZ are compact or not. */
    public static String timestampToString(TimestampNtz timestampNtz) {
        return ChronoUnit.MILLIS
                .addTo(EPOCH, timestampNtz.getMillisecond())
                .plusNanos(timestampNtz.getNanoOfMillisecond())
                .toLocalDateTime()
                .atOffset(ZoneOffset.UTC)
                .format(TimestampFormatter);
    }

    public static String timestampToString(TimestampLtz timestampLtz) {
        return ChronoUnit.MILLIS
                .addTo(EPOCH, timestampLtz.getEpochMillisecond())
                .plusNanos(timestampLtz.getNanoOfMillisecond())
                .toLocalDateTime()
                .atOffset(ZoneOffset.UTC)
                .format(TimestampFormatter);
    }

    /** Parses a hex string back to a byte array. */
    public static byte[] parseHexString(String hex) {
        if ((hex.length() & 1) != 0) {
            throw new IllegalArgumentException("Hex partition value must have an even length.");
        }
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < hex.length(); i += 2) {
            int high = Character.digit(hex.charAt(i), 16);
            int low = Character.digit(hex.charAt(i + 1), 16);
            if (high < 0 || low < 0) {
                throw new IllegalArgumentException("Invalid hex partition value: " + hex);
            }
            bytes[i / 2] = (byte) ((high << 4) + low);
        }
        return bytes;
    }

    /** Parses a formatted date back to days since the epoch. */
    public static int parseDayString(String value) {
        String[] parts = value.split("-");
        LocalDateTime date =
                LocalDateTime.of(
                        Integer.parseInt(parts[0]),
                        Integer.parseInt(parts[1]),
                        Integer.parseInt(parts[2]),
                        0,
                        0);
        return (int) date.toLocalDate().toEpochDay();
    }

    /** Parses a formatted time back to milliseconds of day. */
    public static int parseMilliString(String value) {
        String[] mainParts = value.split("_");
        String[] timeParts = mainParts[0].split("-");
        return Integer.parseInt(timeParts[0]) * 3_600_000
                + Integer.parseInt(timeParts[1]) * 60_000
                + Integer.parseInt(timeParts[2]) * 1_000
                + Integer.parseInt(mainParts[1]);
    }

    public static Float parseFloat(String value) {
        if ("NaN".equals(value)) {
            return Float.NaN;
        } else if ("Inf".equals(value)) {
            return Float.POSITIVE_INFINITY;
        } else if ("-Inf".equals(value)) {
            return Float.NEGATIVE_INFINITY;
        }
        return Float.parseFloat(value.replace("_", "."));
    }

    public static Double parseDouble(String value) {
        if ("NaN".equals(value)) {
            return Double.NaN;
        } else if ("Inf".equals(value)) {
            return Double.POSITIVE_INFINITY;
        } else if ("-Inf".equals(value)) {
            return Double.NEGATIVE_INFINITY;
        }
        return Double.parseDouble(value.replace("_", "."));
    }

    public static TimestampNtz parseTimestampNtz(String value) {
        long[] millisAndNano = parseTimestampString(value);
        return TimestampNtz.fromMillis(millisAndNano[0], (int) millisAndNano[1]);
    }

    public static TimestampLtz parseTimestampLtz(String value) {
        long[] millisAndNano = parseTimestampString(value);
        return TimestampLtz.fromEpochMillis(millisAndNano[0], (int) millisAndNano[1]);
    }

    private static long[] parseTimestampString(String value) {
        int underscoreIndex = value.lastIndexOf('_');
        String dateTimePart = underscoreIndex >= 0 ? value.substring(0, underscoreIndex) : value;
        String nanoPart = underscoreIndex >= 0 ? value.substring(underscoreIndex + 1) : "";

        String[] parts = dateTimePart.split("-");
        int hour = parts.length > 3 ? Integer.parseInt(parts[3]) : 0;
        int minute = parts.length > 4 ? Integer.parseInt(parts[4]) : 0;
        int second = parts.length > 5 ? Integer.parseInt(parts[5]) : 0;
        LocalDateTime dateTime =
                LocalDateTime.of(
                        Integer.parseInt(parts[0]),
                        Integer.parseInt(parts[1]),
                        Integer.parseInt(parts[2]),
                        hour,
                        minute,
                        second);
        long epochMillis =
                dateTime.toLocalDate().toEpochDay() * 86_400_000L
                        + dateTime.toLocalTime().toSecondOfDay() * 1_000L;

        int nanoOfMillisecond = 0;
        if (!nanoPart.isEmpty()) {
            StringBuilder paddedNano = new StringBuilder(nanoPart);
            while (paddedNano.length() < 9) {
                paddedNano.append('0');
            }
            long nanoOfSecond = Long.parseLong(paddedNano.toString());
            epochMillis += nanoOfSecond / 1_000_000;
            nanoOfMillisecond = (int) (nanoOfSecond % 1_000_000);
        }
        return new long[] {epochMillis, nanoOfMillisecond};
    }
}
