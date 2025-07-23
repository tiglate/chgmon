package ludo.mentis.aciem.chgmon.repos;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class MonitorRepositoryImplTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    private MonitorRepositoryImpl monitorRepository;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        monitorRepository = new MonitorRepositoryImpl(jdbcTemplate);
    }

    @Test
    void findAll_ValidParameters_ReturnsRows() {
        // Arrange
        String tableName = "test_table";
        String primaryKeyName = "id";
        List<Map<String, Object>> expectedRows = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 1);
        row1.put("name", "Test 1");
        expectedRows.add(row1);
        
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", 2);
        row2.put("name", "Test 2");
        expectedRows.add(row2);
        
        when(jdbcTemplate.queryForList(anyString())).thenReturn(expectedRows);

        // Act
        List<Map<String, Object>> result = monitorRepository.findAll(tableName, primaryKeyName);

        // Assert
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals(expectedRows, result);
        verify(jdbcTemplate).queryForList("SELECT * FROM test_table ORDER BY id");
    }

    @Test
    void findAll_NullTableName_ThrowsIllegalArgumentException() {
        // Arrange
        String tableName = null;
        String primaryKeyName = "id";

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            monitorRepository.findAll(tableName, primaryKeyName);
        });
        assertEquals("Table name cannot be null or empty", exception.getMessage());
        verify(jdbcTemplate, never()).queryForList(anyString());
    }

    @Test
    void findAll_EmptyTableName_ThrowsIllegalArgumentException() {
        // Arrange
        String tableName = "";
        String primaryKeyName = "id";

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            monitorRepository.findAll(tableName, primaryKeyName);
        });
        assertEquals("Table name cannot be null or empty", exception.getMessage());
        verify(jdbcTemplate, never()).queryForList(anyString());
    }

    @Test
    void findAll_NullPrimaryKeyName_ThrowsIllegalArgumentException() {
        // Arrange
        String tableName = "test_table";
        String primaryKeyName = null;

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            monitorRepository.findAll(tableName, primaryKeyName);
        });
        assertEquals("Primary key name cannot be null or empty", exception.getMessage());
        verify(jdbcTemplate, never()).queryForList(anyString());
    }

    @Test
    void findAll_EmptyPrimaryKeyName_ThrowsIllegalArgumentException() {
        // Arrange
        String tableName = "test_table";
        String primaryKeyName = "";

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            monitorRepository.findAll(tableName, primaryKeyName);
        });
        assertEquals("Primary key name cannot be null or empty", exception.getMessage());
        verify(jdbcTemplate, never()).queryForList(anyString());
    }
}