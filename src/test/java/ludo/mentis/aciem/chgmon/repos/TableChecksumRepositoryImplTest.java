package ludo.mentis.aciem.chgmon.repos;

import ludo.mentis.aciem.chgmon.model.TableChecksum;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TableChecksumRepositoryImplTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    private TableChecksumRepositoryImpl tableChecksumRepository;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        tableChecksumRepository = new TableChecksumRepositoryImpl(jdbcTemplate);
    }

    @Test
    void insert_ValidTableChecksum_ReturnsId() {
        // Arrange
        TableChecksum tableChecksum = new TableChecksum();
        tableChecksum.setTableName("test_table");
        tableChecksum.setPrimaryKey(1L);
        tableChecksum.setCrc32(12345L);

        // Mock the KeyHolder to return a specific ID
        when(jdbcTemplate.update(any(), any(GeneratedKeyHolder.class))).thenAnswer(invocation -> {
            KeyHolder keyHolder = invocation.getArgument(1);
            // Use reflection to set the key in the keyHolder
            java.lang.reflect.Field keyField = GeneratedKeyHolder.class.getDeclaredField("keyList");
            keyField.setAccessible(true);
            keyField.set(keyHolder, java.util.Collections.singletonList(java.util.Collections.singletonMap("", 123)));
            return 1;
        });

        // Act
        Integer result = tableChecksumRepository.insert(tableChecksum);

        // Assert
        assertEquals(123, result);
        verify(jdbcTemplate).update(any(), any(GeneratedKeyHolder.class));
    }

    @Test
    void insert_NullTableChecksum_ThrowsIllegalArgumentException() {
        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            tableChecksumRepository.insert(null);
        });
        assertEquals("TableChecksum cannot be null", exception.getMessage());
        verify(jdbcTemplate, never()).update(any(), any(GeneratedKeyHolder.class));
    }

    @Test
    void insert_NullTableName_ThrowsIllegalArgumentException() {
        // Arrange
        TableChecksum tableChecksum = new TableChecksum();
        tableChecksum.setTableName(null);
        tableChecksum.setPrimaryKey(1L);
        tableChecksum.setCrc32(12345L);

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            tableChecksumRepository.insert(tableChecksum);
        });
        assertEquals("Table name cannot be null or empty", exception.getMessage());
        verify(jdbcTemplate, never()).update(any(), any(GeneratedKeyHolder.class));
    }

    @Test
    void insert_EmptyTableName_ThrowsIllegalArgumentException() {
        // Arrange
        TableChecksum tableChecksum = new TableChecksum();
        tableChecksum.setTableName("");
        tableChecksum.setPrimaryKey(1L);
        tableChecksum.setCrc32(12345L);

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            tableChecksumRepository.insert(tableChecksum);
        });
        assertEquals("Table name cannot be null or empty", exception.getMessage());
        verify(jdbcTemplate, never()).update(any(), any(GeneratedKeyHolder.class));
    }

    @Test
    void insert_NullPrimaryKey_ThrowsIllegalArgumentException() {
        // Arrange
        TableChecksum tableChecksum = new TableChecksum();
        tableChecksum.setTableName("test_table");
        tableChecksum.setPrimaryKey(null);
        tableChecksum.setCrc32(12345L);

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            tableChecksumRepository.insert(tableChecksum);
        });
        assertEquals("Primary key cannot be null", exception.getMessage());
        verify(jdbcTemplate, never()).update(any(), any(GeneratedKeyHolder.class));
    }

    @Test
    void insert_NullCrc32_ThrowsIllegalArgumentException() {
        // Arrange
        TableChecksum tableChecksum = new TableChecksum();
        tableChecksum.setTableName("test_table");
        tableChecksum.setPrimaryKey(1L);
        tableChecksum.setCrc32(null);

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            tableChecksumRepository.insert(tableChecksum);
        });
        assertEquals("CRC32 cannot be null", exception.getMessage());
        verify(jdbcTemplate, never()).update(any(), any(GeneratedKeyHolder.class));
    }

    @Test
    void findByTableNameAndPrimaryKey_RecordExists_ReturnsTableChecksum() {
        // Arrange
        String tableName = "test_table";
        Long primaryKey = 1L;
        TableChecksum expectedTableChecksum = new TableChecksum();
        expectedTableChecksum.setId(1);
        expectedTableChecksum.setTableName(tableName);
        expectedTableChecksum.setPrimaryKey(primaryKey);
        expectedTableChecksum.setCrc32(12345L);

        when(jdbcTemplate.queryForObject(
                anyString(),
                any(RowMapper.class),
                eq(tableName),
                eq(primaryKey)
        )).thenReturn(expectedTableChecksum);

        // Act
        TableChecksum result = tableChecksumRepository.findByTableNameAndPrimaryKey(tableName, primaryKey);

        // Assert
        assertNotNull(result);
        assertEquals(expectedTableChecksum.getId(), result.getId());
        assertEquals(expectedTableChecksum.getTableName(), result.getTableName());
        assertEquals(expectedTableChecksum.getPrimaryKey(), result.getPrimaryKey());
        assertEquals(expectedTableChecksum.getCrc32(), result.getCrc32());
        verify(jdbcTemplate).queryForObject(
                eq("SELECT id_table_checksum, table_name, primary_key, crc32 FROM tb_table_checksum WHERE table_name = ? AND primary_key = ?"),
                any(RowMapper.class),
                eq(tableName),
                eq(primaryKey)
        );
    }

    @Test
    void findByTableNameAndPrimaryKey_RecordDoesNotExist_ReturnsNull() {
        // Arrange
        String tableName = "test_table";
        Long primaryKey = 1L;

        when(jdbcTemplate.queryForObject(
                anyString(),
                any(RowMapper.class),
                eq(tableName),
                eq(primaryKey)
        )).thenThrow(new EmptyResultDataAccessException(1));

        // Act
        TableChecksum result = tableChecksumRepository.findByTableNameAndPrimaryKey(tableName, primaryKey);

        // Assert
        assertNull(result);
        verify(jdbcTemplate).queryForObject(
                eq("SELECT id_table_checksum, table_name, primary_key, crc32 FROM tb_table_checksum WHERE table_name = ? AND primary_key = ?"),
                any(RowMapper.class),
                eq(tableName),
                eq(primaryKey)
        );
    }

    @Test
    void findByTableNameAndPrimaryKey_NullPrimaryKey_ThrowsIllegalArgumentException() {
        // Arrange
        String tableName = "test_table";
        Long primaryKey = null;

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            tableChecksumRepository.findByTableNameAndPrimaryKey(tableName, primaryKey);
        });
        assertEquals("Primary key cannot be null", exception.getMessage());
        verify(jdbcTemplate, never()).queryForObject(anyString(), any(RowMapper.class), any(), any());
    }

    @Test
    void update_ValidTableChecksum_ReturnsTrue() {
        // Arrange
        TableChecksum tableChecksum = new TableChecksum();
        tableChecksum.setId(1);
        tableChecksum.setTableName("test_table");
        tableChecksum.setPrimaryKey(1L);
        tableChecksum.setCrc32(12345L);

        when(jdbcTemplate.update(
                anyString(),
                eq(tableChecksum.getTableName()),
                eq(tableChecksum.getPrimaryKey()),
                eq(tableChecksum.getCrc32()),
                eq(tableChecksum.getId())
        )).thenReturn(1);

        // Act
        boolean result = tableChecksumRepository.update(tableChecksum);

        // Assert
        assertTrue(result);
        verify(jdbcTemplate).update(
                eq("UPDATE tb_table_checksum SET table_name = ?, primary_key = ?, crc32 = ? WHERE id_table_checksum = ?"),
                eq(tableChecksum.getTableName()),
                eq(tableChecksum.getPrimaryKey()),
                eq(tableChecksum.getCrc32()),
                eq(tableChecksum.getId())
        );
    }

    @Test
    void update_NoRowsAffected_ReturnsFalse() {
        // Arrange
        TableChecksum tableChecksum = new TableChecksum();
        tableChecksum.setId(1);
        tableChecksum.setTableName("test_table");
        tableChecksum.setPrimaryKey(1L);
        tableChecksum.setCrc32(12345L);

        when(jdbcTemplate.update(
                anyString(),
                eq(tableChecksum.getTableName()),
                eq(tableChecksum.getPrimaryKey()),
                eq(tableChecksum.getCrc32()),
                eq(tableChecksum.getId())
        )).thenReturn(0);

        // Act
        boolean result = tableChecksumRepository.update(tableChecksum);

        // Assert
        assertFalse(result);
        verify(jdbcTemplate).update(
                eq("UPDATE tb_table_checksum SET table_name = ?, primary_key = ?, crc32 = ? WHERE id_table_checksum = ?"),
                eq(tableChecksum.getTableName()),
                eq(tableChecksum.getPrimaryKey()),
                eq(tableChecksum.getCrc32()),
                eq(tableChecksum.getId())
        );
    }

    @Test
    void update_NullTableChecksum_ThrowsIllegalArgumentException() {
        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            tableChecksumRepository.update(null);
        });
        assertEquals("TableChecksum cannot be null", exception.getMessage());
        verify(jdbcTemplate, never()).update(anyString(), any(), any(), any(), any());
    }

    @Test
    void update_NullId_ThrowsIllegalArgumentException() {
        // Arrange
        TableChecksum tableChecksum = new TableChecksum();
        tableChecksum.setId(null);
        tableChecksum.setTableName("test_table");
        tableChecksum.setPrimaryKey(1L);
        tableChecksum.setCrc32(12345L);

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            tableChecksumRepository.update(tableChecksum);
        });
        assertEquals("TableChecksum ID cannot be null", exception.getMessage());
        verify(jdbcTemplate, never()).update(anyString(), any(), any(), any(), any());
    }

    @Test
    void update_NullTableName_ThrowsIllegalArgumentException() {
        // Arrange
        TableChecksum tableChecksum = new TableChecksum();
        tableChecksum.setId(1);
        tableChecksum.setTableName(null);
        tableChecksum.setPrimaryKey(1L);
        tableChecksum.setCrc32(12345L);

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            tableChecksumRepository.update(tableChecksum);
        });
        assertEquals("Table name cannot be null or empty", exception.getMessage());
        verify(jdbcTemplate, never()).update(anyString(), any(), any(), any(), any());
    }

    @Test
    void update_EmptyTableName_ThrowsIllegalArgumentException() {
        // Arrange
        TableChecksum tableChecksum = new TableChecksum();
        tableChecksum.setId(1);
        tableChecksum.setTableName("");
        tableChecksum.setPrimaryKey(1L);
        tableChecksum.setCrc32(12345L);

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            tableChecksumRepository.update(tableChecksum);
        });
        assertEquals("Table name cannot be null or empty", exception.getMessage());
        verify(jdbcTemplate, never()).update(anyString(), any(), any(), any(), any());
    }

    @Test
    void update_NullPrimaryKey_ThrowsIllegalArgumentException() {
        // Arrange
        TableChecksum tableChecksum = new TableChecksum();
        tableChecksum.setId(1);
        tableChecksum.setTableName("test_table");
        tableChecksum.setPrimaryKey(null);
        tableChecksum.setCrc32(12345L);

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            tableChecksumRepository.update(tableChecksum);
        });
        assertEquals("Primary key cannot be null", exception.getMessage());
        verify(jdbcTemplate, never()).update(anyString(), any(), any(), any(), any());
    }

    @Test
    void update_NullCrc32_ThrowsIllegalArgumentException() {
        // Arrange
        TableChecksum tableChecksum = new TableChecksum();
        tableChecksum.setId(1);
        tableChecksum.setTableName("test_table");
        tableChecksum.setPrimaryKey(1L);
        tableChecksum.setCrc32(null);

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            tableChecksumRepository.update(tableChecksum);
        });
        assertEquals("CRC32 cannot be null", exception.getMessage());
        verify(jdbcTemplate, never()).update(anyString(), any(), any(), any(), any());
    }
}