package com.connectgroup;

import java.io.BufferedReader;
import java.io.Reader;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class DataFilterer {

    private static final int TS_LENGTH = 10;
    private static final int COUNTRY_LENGTH = 2;
    private static final int RESPONSE_LENGTH = 3;
    static Predicate<String> LINE_PREDICATE = l -> l != null && !l.trim().equals("");
    static Predicate<String> HEADER_TIMESTAMP_FILTER = l -> !l.contains("REQUEST_TIMESTAMP");
    static Predicate<String> HEADER_COUNTRY_CODE_FILTER = l -> !l.contains("COUNTRY_CODE");
    static Predicate<String> HEADER_RESPONSE_TIME_FILTER = l -> !l.contains("RESPONSE_TIME");
    static Predicate<String> GENERAL_FILTER = LINE_PREDICATE
            .and(HEADER_COUNTRY_CODE_FILTER)
            .and(HEADER_RESPONSE_TIME_FILTER)
            .and(HEADER_TIMESTAMP_FILTER);

    public static Collection<?> filterByCountry(Reader source, String country) {
        return getLines(source)
                .stream()
                .filter(d -> country.equals(d.country))
                .collect(Collectors.toList());
    }

    public static Collection<?> filterByCountryWithResponseTimeAboveLimit(Reader source, String country, long limit) {
        return getLines(source)
                .stream()
                .filter(d -> country.equals(d.country))
                .filter(d -> d.response > limit)
                .collect(Collectors.toList());
    }

    public static Collection<?> filterByResponseTimeAboveAverage(Reader source) {
        List<DataObject> lines = getLines(source);
        //Couldnt find a better way to use one stream to apply average.
        Double average = lines
                .stream()
                .collect(Collectors.averagingInt(d -> d.response));
        return lines.stream()
                .filter(l -> l.response > average)
                .collect(Collectors.toList());
    }

    private static List<DataObject> getLines(Reader source) {
        return Optional.ofNullable(source)
                .map(s -> new BufferedReader(s))
                .map((BufferedReader br) -> getFilteredLines(br))
                .orElseGet(Collections::emptyList);
    }

    private static List<DataObject> getFilteredLines(BufferedReader br) {
        return br.lines()
                .filter(GENERAL_FILTER)
                .map(DataFilterer::toDataObject)
                .collect(Collectors.toList());
    }

    private static DataObject toDataObject(String line) {
        String[] fields = line.split(",");

        //find ts
        String ts = fields[0].length() == TS_LENGTH ? fields[0] :
                fields[1].length() == TS_LENGTH ? fields[1] : fields[2];
        String cc = fields[0].length() == COUNTRY_LENGTH ? fields[0] :
                fields[1].length() == COUNTRY_LENGTH ? fields[1] : fields[2];
        String res = fields[0].length() == RESPONSE_LENGTH ? fields[0] :
                fields[1].length() == RESPONSE_LENGTH ? fields[1] : fields[2];
        long timestamp = Long.parseLong(ts);
        String country = cc;
        int response = Integer.parseInt(res);
        return new DataObject(timestamp, country, response);
    }

    private static class DataObject {
        long timestamp;
        String country;
        int response;

        public DataObject(long timestamp, String country, int response) {
            this.timestamp = timestamp;
            this.country = country;
            this.response = response;
        }
    }
}