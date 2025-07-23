package ludo.mentis.aciem.chgmon.repos;

import ludo.mentis.aciem.chgmon.model.Audit;
import ludo.mentis.aciem.chgmon.model.ChangeType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AuditRepositoryImplTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    private AuditRepositoryImpl auditRepository;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        auditRepository = new AuditRepositoryImpl(jdbcTemplate);
    }

    @Test
    void insert_ValidAudit_ReturnsId() {
        // Arrange
        Audit audit = new Audit();
        audit.setTableName("test_table");
        audit.setPrimaryKey(1L);
        audit.setChangeType(ChangeType.INSERT);
        audit.setChangeDate(LocalDateTime.now());

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
        Integer result = auditRepository.insert(audit);

        // Assert
        assertEquals(123, result);
        verify(jdbcTemplate).update(any(), any(GeneratedKeyHolder.class));
    }

    @Test
    void insert_NullAudit_ThrowsIllegalArgumentException() {
        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            auditRepository.insert(null);
        });
        assertEquals("Audit cannot be null", exception.getMessage());
        verify(jdbcTemplate, never()).update(any(), any(GeneratedKeyHolder.class));
    }

    @Test
    void insert_NullTableName_ThrowsIllegalArgumentException() {
        // Arrange
        Audit audit = new Audit();
        audit.setTableName(null);
        audit.setPrimaryKey(1L);
        audit.setChangeType(ChangeType.INSERT);

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            auditRepository.insert(audit);
        });
        assertEquals("Table name cannot be null or empty", exception.getMessage());
        verify(jdbcTemplate, never()).update(any(), any(GeneratedKeyHolder.class));
    }

    @Test
    void insert_EmptyTableName_ThrowsIllegalArgumentException() {
        // Arrange
        Audit audit = new Audit();
        audit.setTableName("");
        audit.setPrimaryKey(1L);
        audit.setChangeType(ChangeType.INSERT);

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            auditRepository.insert(audit);
        });
        assertEquals("Table name cannot be null or empty", exception.getMessage());
        verify(jdbcTemplate, never()).update(any(), any(GeneratedKeyHolder.class));
    }

    @Test
    void insert_NullPrimaryKey_ThrowsIllegalArgumentException() {
        // Arrange
        Audit audit = new Audit();
        audit.setTableName("test_table");
        audit.setPrimaryKey(null);
        audit.setChangeType(ChangeType.INSERT);

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            auditRepository.insert(audit);
        });
        assertEquals("Primary key cannot be null", exception.getMessage());
        verify(jdbcTemplate, never()).update(any(), any(GeneratedKeyHolder.class));
    }

    @Test
    void insert_NullChangeType_ThrowsIllegalArgumentException() {
        // Arrange
        Audit audit = new Audit();
        audit.setTableName("test_table");
        audit.setPrimaryKey(1L);
        audit.setChangeType(null);

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            auditRepository.insert(audit);
        });
        assertEquals("Change type cannot be null", exception.getMessage());
        verify(jdbcTemplate, never()).update(any(), any(GeneratedKeyHolder.class));
    }

    @Test
    void insert_WithChangeDate_UsesProvidedDate() {
        // Arrange
        Audit audit = new Audit();
        audit.setTableName("test_table");
        audit.setPrimaryKey(1L);
        audit.setChangeType(ChangeType.INSERT);
        LocalDateTime changeDate = LocalDateTime.of(2025, 7, 22, 23, 31);
        audit.setChangeDate(changeDate);

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
        Integer result = auditRepository.insert(audit);

        // Assert
        assertEquals(123, result);
        verify(jdbcTemplate).update(any(), any(GeneratedKeyHolder.class));
    }

    @Test
    void insert_WithoutChangeDate_UsesCurrentTime() {
        // Arrange
        Audit audit = new Audit();
        audit.setTableName("test_table");
        audit.setPrimaryKey(1L);
        audit.setChangeType(ChangeType.INSERT);
        audit.setChangeDate(null);

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
        Integer result = auditRepository.insert(audit);

        // Assert
        assertEquals(123, result);
        verify(jdbcTemplate).update(any(), any(GeneratedKeyHolder.class));
    }

    @Test
    void isAlreadyDeleted_RecordExists_ReturnsTrue() {
        // Arrange
        String tableName = "test_table";
        Long primaryKey = 1L;
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), eq(tableName), eq(primaryKey))).thenReturn(1);

        // Act
        boolean result = auditRepository.isAlreadyDeleted(tableName, primaryKey);

        // Assert
        assertTrue(result);
        verify(jdbcTemplate).queryForObject(
            "SELECT COUNT(*) FROM tb_audit WHERE table_name = ? AND primary_key = ? AND change_type = 'DELETE'", 
            Integer.class, 
            tableName, 
            primaryKey
        );
    }

    @Test
    void isAlreadyDeleted_RecordDoesNotExist_ReturnsFalse() {
        // Arrange
        String tableName = "test_table";
        Long primaryKey = 1L;
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), eq(tableName), eq(primaryKey))).thenReturn(0);

        // Act
        boolean result = auditRepository.isAlreadyDeleted(tableName, primaryKey);

        // Assert
        assertFalse(result);
        verify(jdbcTemplate).queryForObject(
            "SELECT COUNT(*) FROM tb_audit WHERE table_name = ? AND primary_key = ? AND change_type = 'DELETE'", 
            Integer.class, 
            tableName, 
            primaryKey
        );
    }

    @Test
    void isAlreadyDeleted_NullResult_ReturnsFalse() {
        // Arrange
        String tableName = "test_table";
        Long primaryKey = 1L;
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), eq(tableName), eq(primaryKey))).thenReturn(null);

        // Act
        boolean result = auditRepository.isAlreadyDeleted(tableName, primaryKey);

        // Assert
        assertFalse(result);
        verify(jdbcTemplate).queryForObject(
            "SELECT COUNT(*) FROM tb_audit WHERE table_name = ? AND primary_key = ? AND change_type = 'DELETE'", 
            Integer.class, 
            tableName, 
            primaryKey
        );
    }
}