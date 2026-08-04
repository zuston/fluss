/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.utils;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypeRoot;

import java.time.Instant;
import java.time.LocalDate;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.metadata.TablePath.detectInvalidName;
import static org.apache.fluss.metadata.TablePath.validatePrefix;

/** Utils for partition. */
public class PartitionUtils {

    public static final String HISTORICAL_PARTITION_VALUE = "__historical__";

    public static final List<DataTypeRoot> PARTITION_KEY_SUPPORTED_TYPES =
            Arrays.asList(
                    DataTypeRoot.CHAR,
                    DataTypeRoot.STRING,
                    DataTypeRoot.BOOLEAN,
                    DataTypeRoot.BINARY,
                    DataTypeRoot.BYTES,
                    DataTypeRoot.TINYINT,
                    DataTypeRoot.SMALLINT,
                    DataTypeRoot.INTEGER,
                    DataTypeRoot.DATE,
                    DataTypeRoot.TIME_WITHOUT_TIME_ZONE,
                    DataTypeRoot.BIGINT,
                    DataTypeRoot.FLOAT,
                    DataTypeRoot.DOUBLE,
                    DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                    DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

    private static final String YEAR_FORMAT = "yyyy";
    private static final String QUARTER_FORMAT = "yyyyQ";
    private static final String MONTH_FORMAT = "yyyyMM";
    private static final String DAY_FORMAT = "yyyyMMdd";
    private static final String HOUR_FORMAT = "yyyyMMddHH";

    public static void validatePartitionSpec(
            TablePath tablePath,
            List<String> partitionKeys,
            PartitionSpec partitionSpec,
            boolean isCreate) {
        Map<String, String> partitionSpecMap = partitionSpec.getSpecMap();
        if (partitionKeys.size() != partitionSpecMap.size()) {
            throw new InvalidPartitionException(
                    String.format(
                            "PartitionSpec size is not equal to partition keys size for partitioned table %s.",
                            tablePath));
        }

        List<String> reOrderedPartitionValues = new ArrayList<>(partitionKeys.size());
        for (String partitionKey : partitionKeys) {
            if (!partitionSpecMap.containsKey(partitionKey)) {
                throw new InvalidPartitionException(
                        String.format(
                                "PartitionSpec %s does not contain partition key '%s' for partitioned table %s.",
                                partitionSpec, partitionKey, tablePath));
            } else {
                reOrderedPartitionValues.add(partitionSpecMap.get(partitionKey));
            }
        }

        validatePartitionValues(reOrderedPartitionValues, isCreate);
    }

    @VisibleForTesting
    static void validatePartitionValues(List<String> partitionValues, boolean isCreate) {
        for (String value : partitionValues) {
            String invalidNameError = detectInvalidName(value);
            if (invalidNameError != null || (isCreate && validatePrefix(value) != null)) {
                throw new InvalidPartitionException(
                        "The partition value "
                                + value
                                + " is invalid: "
                                + (invalidNameError != null
                                        ? invalidNameError
                                        : validatePrefix(value)));
            }
        }
    }

    /**
     * Returns whether a valid auto-partition value is earlier than the partition containing {@code
     * now}.
     */
    public static boolean isPastAutoPartition(
            String partitionTime, AutoPartitionStrategy autoPartitionStrategy, Instant now) {
        AutoPartitionTimeUnit timeUnit = autoPartitionStrategy.timeUnit();
        if (!isValidPartitionTime(partitionTime, timeUnit)) {
            return false;
        }
        ZonedDateTime current =
                ZonedDateTime.ofInstant(now, autoPartitionStrategy.timeZone().toZoneId());
        String currentPartitionTime = generateAutoPartitionTime(current, 0, timeUnit);
        return partitionTime.compareTo(currentPartitionTime) < 0;
    }

    private static boolean isValidPartitionTime(
            String partitionTime, AutoPartitionTimeUnit timeUnit) {
        try {
            switch (timeUnit) {
                case YEAR:
                    Year.parse(partitionTime, DateTimeFormatter.ofPattern("uuuu"));
                    break;
                case QUARTER:
                    if (!partitionTime.matches("\\d{4}[1-4]")) {
                        return false;
                    }
                    break;
                case MONTH:
                    YearMonth.parse(partitionTime, DateTimeFormatter.ofPattern("uuuuMM"));
                    break;
                case DAY:
                    LocalDate.parse(partitionTime, DateTimeFormatter.BASIC_ISO_DATE);
                    break;
                case HOUR:
                    if (!partitionTime.matches("\\d{10}")) {
                        return false;
                    }
                    LocalDate.parse(
                            partitionTime.substring(0, 8), DateTimeFormatter.BASIC_ISO_DATE);
                    int hour = Integer.parseInt(partitionTime.substring(8));
                    if (hour > 23) {
                        return false;
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported time unit: " + timeUnit);
            }
            return true;
        } catch (DateTimeParseException | NumberFormatException e) {
            return false;
        }
    }

    /**
     * Generate {@link ResolvedPartitionSpec} for auto partition in server. When we auto creating a
     * partition, we need to first generate a {@link ResolvedPartitionSpec}.
     *
     * <p>The value is the formatted time with the specified time unit.
     *
     * @param partitionKeys the partition keys
     * @param current the current time
     * @param offset the offset
     * @param timeUnit the time unit
     * @return the resolved partition spec
     */
    public static ResolvedPartitionSpec generateAutoPartition(
            List<String> partitionKeys,
            ZonedDateTime current,
            int offset,
            AutoPartitionTimeUnit timeUnit) {
        String autoPartitionFieldSpec = generateAutoPartitionTime(current, offset, timeUnit);

        return ResolvedPartitionSpec.fromPartitionName(partitionKeys, autoPartitionFieldSpec);
    }

    public static String generateAutoPartitionTime(
            ZonedDateTime current, int offset, AutoPartitionTimeUnit timeUnit) {
        String autoPartitionFieldSpec;
        switch (timeUnit) {
            case YEAR:
                autoPartitionFieldSpec = getFormattedTime(current.plusYears(offset), YEAR_FORMAT);
                break;
            case QUARTER:
                autoPartitionFieldSpec =
                        getFormattedTime(current.plusMonths(offset * 3L), QUARTER_FORMAT);
                break;
            case MONTH:
                autoPartitionFieldSpec = getFormattedTime(current.plusMonths(offset), MONTH_FORMAT);
                break;
            case DAY:
                autoPartitionFieldSpec = getFormattedTime(current.plusDays(offset), DAY_FORMAT);
                break;
            case HOUR:
                autoPartitionFieldSpec = getFormattedTime(current.plusHours(offset), HOUR_FORMAT);
                break;
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + timeUnit);
        }
        return autoPartitionFieldSpec;
    }

    private static String getFormattedTime(ZonedDateTime zonedDateTime, String format) {
        return DateTimeFormatter.ofPattern(format).format(zonedDateTime);
    }

    /** Parses a partition value back to its typed Fluss internal representation. */
    public static Object parseValueOfType(String value, DataTypeRoot type) {
        switch (type) {
            case CHAR:
            case STRING:
                return BinaryString.fromString(value);
            case BOOLEAN:
                if ("true".equalsIgnoreCase(value)) {
                    return true;
                } else if ("false".equalsIgnoreCase(value)) {
                    return false;
                }
                throw new IllegalArgumentException(
                        "Invalid boolean partition value: '"
                                + value
                                + "'. Expected 'true' or 'false'.");
            case BINARY:
            case BYTES:
                return PartitionNameConverters.parseHexString(value);
            case TINYINT:
                return Byte.parseByte(value);
            case SMALLINT:
                return Short.parseShort(value);
            case INTEGER:
                return Integer.parseInt(value);
            case BIGINT:
                return Long.parseLong(value);
            case DATE:
                return PartitionNameConverters.parseDayString(value);
            case TIME_WITHOUT_TIME_ZONE:
                return PartitionNameConverters.parseMilliString(value);
            case FLOAT:
                return PartitionNameConverters.parseFloat(value);
            case DOUBLE:
                return PartitionNameConverters.parseDouble(value);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return PartitionNameConverters.parseTimestampNtz(value);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return PartitionNameConverters.parseTimestampLtz(value);
            default:
                throw new IllegalArgumentException("Unsupported DataTypeRoot: " + type);
        }
    }

    public static String convertValueOfType(Object value, DataTypeRoot type) {
        String stringPartitionKey = "";
        switch (type) {
            case CHAR:
            case STRING:
                stringPartitionKey = ((BinaryString) value).toString();
                break;
            case BOOLEAN:
                Boolean booleanValue = (Boolean) value;
                stringPartitionKey = booleanValue.toString();
                break;
            case BINARY:
            case BYTES:
                byte[] bytesValue = (byte[]) value;
                stringPartitionKey = PartitionNameConverters.hexString(bytesValue);
                break;
            case TINYINT:
                Byte tinyIntValue = (Byte) value;
                stringPartitionKey = tinyIntValue.toString();
                break;
            case SMALLINT:
                Short smallIntValue = (Short) value;
                stringPartitionKey = smallIntValue.toString();
                break;
            case INTEGER:
                Integer intValue = (Integer) value;
                stringPartitionKey = intValue.toString();
                break;
            case BIGINT:
                Long bigIntValue = (Long) value;
                stringPartitionKey = bigIntValue.toString();
                break;
            case DATE:
                Integer dateValue = (Integer) value;
                stringPartitionKey = PartitionNameConverters.dayToString(dateValue);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                Integer timeValue = (Integer) value;
                stringPartitionKey = PartitionNameConverters.milliToString(timeValue);
                break;
            case FLOAT:
                Float floatValue = (Float) value;
                stringPartitionKey = PartitionNameConverters.reformatFloat(floatValue);
                break;
            case DOUBLE:
                Double doubleValue = (Double) value;
                stringPartitionKey = PartitionNameConverters.reformatDouble(doubleValue);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                TimestampLtz timeStampLTZValue = (TimestampLtz) value;
                stringPartitionKey = PartitionNameConverters.timestampToString(timeStampLTZValue);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampNtz timeStampNTZValue = (TimestampNtz) value;
                stringPartitionKey = PartitionNameConverters.timestampToString(timeStampNTZValue);
                break;
            default:
                throw new IllegalArgumentException("Unsupported DataTypeRoot: " + type);
        }
        return stringPartitionKey;
    }
}
