package ludo.mentis.aciem.chgmon.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ChecksumServiceImplTest {

    private ChecksumServiceImpl checksumService;

    @BeforeEach
    void setUp() {
        checksumService = new ChecksumServiceImpl();
    }

    @Test
    void calculate_EmptyMap_ReturnsChecksum() {
        // Arrange
        Map<String, Object> emptyMap = new HashMap<>();

        // Act
        long result = checksumService.calculate(emptyMap);

        // Assert
        assertEquals(0, result, "Empty map should return a checksum of 0");
    }

    @Test
    void calculate_NullValue_HandlesCorrectly() {
        // Arrange
        Map<String, Object> mapWithNull = new HashMap<>();
        mapWithNull.put("key1", null);
        mapWithNull.put("key2", "value2");

        // Act
        long result = checksumService.calculate(mapWithNull);

        // Assert
        assertNotEquals(0, result, "Map with null value should produce a non-zero checksum");
    }

    @Test
    void calculate_DifferentDataTypes_HandlesCorrectly() {
        // Arrange
        Map<String, Object> mapWithDifferentTypes = new HashMap<>();
        mapWithDifferentTypes.put("stringKey", "stringValue");
        mapWithDifferentTypes.put("intKey", 123);
        mapWithDifferentTypes.put("doubleKey", 123.456);
        mapWithDifferentTypes.put("booleanKey", true);

        // Act
        long result = checksumService.calculate(mapWithDifferentTypes);

        // Assert
        assertNotEquals(0, result, "Map with different data types should produce a non-zero checksum");
    }

    @Test
    void calculate_IdenticalMaps_ReturnsSameChecksum() {
        // Arrange
        Map<String, Object> map1 = new HashMap<>();
        map1.put("key1", "value1");
        map1.put("key2", 123);

        Map<String, Object> map2 = new HashMap<>();
        map2.put("key1", "value1");
        map2.put("key2", 123);

        // Act
        long result1 = checksumService.calculate(map1);
        long result2 = checksumService.calculate(map2);

        // Assert
        assertEquals(result1, result2, "Identical maps should produce the same checksum");
    }

    @Test
    void calculate_DifferentMaps_ReturnsDifferentChecksums() {
        // Arrange
        Map<String, Object> map1 = new HashMap<>();
        map1.put("key1", "value1");
        map1.put("key2", 123);

        Map<String, Object> map2 = new HashMap<>();
        map2.put("key1", "value2");
        map2.put("key2", 123);

        // Act
        long result1 = checksumService.calculate(map1);
        long result2 = checksumService.calculate(map2);

        // Assert
        assertNotEquals(result1, result2, "Different maps should produce different checksums");
    }

    @Test
    void calculate_MapOrderDoesNotMatter_ReturnsDifferentChecksums() {
        // Arrange
        // Using LinkedHashMap to ensure consistent iteration order
        Map<String, Object> map1 = new LinkedHashMap<>();
        map1.put("key1", "value1");
        map1.put("key2", "value2");

        Map<String, Object> map2 = new LinkedHashMap<>();
        map2.put("key2", "value2");
        map2.put("key1", "value1");

        // Act
        long result1 = checksumService.calculate(map1);
        long result2 = checksumService.calculate(map2);

        // Assert
        // Note: This test verifies the current implementation behavior
        // The current implementation is order-dependent due to how it processes entries
        assertNotEquals(result1, result2, "Maps with same entries in different order produce different checksums");
    }

    @Test
    void calculate_NullMap_ThrowsException() {
        // Act & Assert
        assertThrows(NullPointerException.class, () -> {
            checksumService.calculate(null);
        }, "Null map should throw NullPointerException");
    }

    @Test
    void calculate_ConsistentResults_SameInputProducesSameOutput() {
        // Arrange
        Map<String, Object> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", 123);

        // Act
        long result1 = checksumService.calculate(map);
        long result2 = checksumService.calculate(map);
        long result3 = checksumService.calculate(map);

        // Assert
        assertEquals(result1, result2, "Multiple calculations on the same map should be consistent");
        assertEquals(result2, result3, "Multiple calculations on the same map should be consistent");
    }

    @Test
    void calculate_LargeMap_HandlesCorrectly() {
        // Arrange
        Map<String, Object> largeMap = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            largeMap.put("key" + i, "value" + i);
        }

        // Act
        long result = checksumService.calculate(largeMap);

        // Assert
        assertNotEquals(0, result, "Large map should produce a non-zero checksum");
    }
}