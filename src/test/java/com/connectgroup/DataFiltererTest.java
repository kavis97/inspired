package com.connectgroup;

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collection;

import static org.junit.Assert.*;

public class DataFiltererTest {
    private static final String RESOURCES = "src/test/resources/";
    private static final String SINGLE_LINE = "single-line";
    private static final String MULTI_LINES = "multi-lines";
    private static final String SINGLE_LINE_DIFFERENT_ORDER = "single-line-different-order";
    private static final String EMPTY = "empty";
    private static final String NO_DATA_LINE = "no-data-line";
    private static final String GB = "GB";
    private static final String US = "US";

    @Test
    public void shouldReturnEmptyCollection_WhenLogFileIsEmpty() throws FileNotFoundException {
        assertTrue(DataFilterer.filterByCountry(openFile(RESOURCES + EMPTY), GB).isEmpty());
    }

    @Test
    public void shouldReturnEmptyCollection_WhenLogFileIsEmpty1() throws FileNotFoundException {
        assertTrue(DataFilterer.filterByCountry(openFile(RESOURCES + NO_DATA_LINE), GB).isEmpty());
    }

    @Test
    public void shouldReturnCollection_WhenLogFileIsNotEmpty() throws FileNotFoundException {
        assertFalse(DataFilterer.filterByCountry(openFile(RESOURCES + SINGLE_LINE), GB).isEmpty());
    }

    @Test
    public void shouldReturnEmptyCollection_WhenCountryCodeNotMatches() throws FileNotFoundException {
        assertTrue(DataFilterer.filterByCountry(openFile(RESOURCES + SINGLE_LINE), US).isEmpty());
    }

    @Test
    public void shouldReturnCollectionFilteringCommonHeaders() throws FileNotFoundException {
        Collection<?> result = DataFilterer.filterByCountry(openFile(RESOURCES + SINGLE_LINE), GB);
        assertFalse(result.isEmpty());
        assertEquals(1, result.size());

        result = DataFilterer.filterByCountry(openFile(RESOURCES + MULTI_LINES), US);
        assertFalse(result.isEmpty());
        assertEquals(3, result.size());
    }

    @Test
    public void shouldReturnCollection_WhenHeadersAreInAnyOrder() throws FileNotFoundException {
        Collection<?> result = DataFilterer.filterByCountry(openFile(RESOURCES + SINGLE_LINE_DIFFERENT_ORDER), GB);
        assertFalse(result.isEmpty());
        assertEquals(1, result.size());
    }

    @Test
    public void shouldReturnEmptyCollection_WhenResponseTimeNotAboveLimit() throws FileNotFoundException {
        assertTrue(DataFilterer.filterByCountryWithResponseTimeAboveLimit(openFile(RESOURCES + SINGLE_LINE), GB, 201).isEmpty());
    }

    @Test
    public void shouldReturnCollection_WhenResponseTimeAboveLimit() throws FileNotFoundException {
        Collection<?> result = DataFilterer.filterByCountryWithResponseTimeAboveLimit(openFile(RESOURCES + MULTI_LINES), US, 540);
        assertFalse(result.isEmpty());
        assertEquals(2, result.size());
    }

    @Test
    public void shouldReturnCollection_WhenResponseTimeAboveAverage() throws FileNotFoundException {
        Collection<?> result = DataFilterer.filterByResponseTimeAboveAverage(openFile(RESOURCES + MULTI_LINES));
        assertFalse(result.isEmpty());
        assertEquals(3, result.size());
    }

    private FileReader openFile(String filename) throws FileNotFoundException {
        return new FileReader(new File(filename));
    }
}
