package ludo.mentis.aciem.chgmon.repos;

import ludo.mentis.aciem.chgmon.model.TableChecksum;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
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

    @Test
    void findDeletedRows_RecordExists_ReturnsTableChecksum() {
        // Arrange
        String tableName = "test_table";
        String primaryKeyName = "id";
        
        // Create expected TableChecksum
        TableChecksum expectedTableChecksum = new TableChecksum();
        expectedTableChecksum.setId(1);
        expectedTableChecksum.setTableName(tableName);
        expectedTableChecksum.setPrimaryKey(123L);
        expectedTableChecksum.setCrc32(456L);
        
        // Mock the queryForObject method to return the expected TableChecksum
        when(jdbcTemplate.queryForObject(
                anyString(),
                any(RowMapper.class)
        )).thenReturn(expectedTableChecksum);
        
        // Act
        TableChecksum result = monitorRepository.findDeletedRows(tableName, primaryKeyName);
        
        // Assert
        assertNotNull(result);
        assertEquals(expectedTableChecksum.getId(), result.getId());
        assertEquals(expectedTableChecksum.getTableName(), result.getTableName());
        assertEquals(expectedTableChecksum.getPrimaryKey(), result.getPrimaryKey());
        assertEquals(expectedTableChecksum.getCrc32(), result.getCrc32());
        
        // Verify the SQL query format
        verify(jdbcTemplate).queryForObject(
                contains("SELECT * FROM tb_table_checksum WHERE table_name = '" + tableName + "' AND primary_key NOT IN (SELECT " + primaryKeyName + " FROM " + tableName + ")"),
                any(RowMapper.class)
        );
    }
    
    @Test
    void findDeletedRows_NoRecordFound_ReturnsNull() {
        // Arrange
        String tableName = "test_table";
        String primaryKeyName = "id";
        
        // Mock the queryForObject method to throw EmptyResultDataAccessException
        when(jdbcTemplate.queryForObject(
                anyString(),
                any(RowMapper.class)
        )).thenThrow(new EmptyResultDataAccessException(1));
        
        // Act
        TableChecksum result = monitorRepository.findDeletedRows(tableName, primaryKeyName);
        
        // Assert
        assertNull(result);
        
        // Verify the SQL query format
        verify(jdbcTemplate).queryForObject(
                contains("SELECT * FROM tb_table_checksum WHERE table_name = '" + tableName + "' AND primary_key NOT IN (SELECT " + primaryKeyName + " FROM " + tableName + ")"),
                any(RowMapper.class)
        );
    }
}