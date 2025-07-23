package ludo.mentis.aciem.chgmon.task;

import ludo.mentis.aciem.chgmon.config.MonitorProperties;
import ludo.mentis.aciem.chgmon.model.Audit;
import ludo.mentis.aciem.chgmon.model.ChangeType;
import ludo.mentis.aciem.chgmon.model.TableChecksum;
import ludo.mentis.aciem.chgmon.repos.AuditRepository;
import ludo.mentis.aciem.chgmon.repos.MonitorRepository;
import ludo.mentis.aciem.chgmon.repos.TableChecksumRepository;
import ludo.mentis.aciem.chgmon.service.ChecksumService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MonitorTaskTest {

    @Mock
    private ChecksumService checksumService;

    @Mock
    private AuditRepository auditRepository;

    @Mock
    private MonitorRepository monitorRepository;

    @Mock
    private TableChecksumRepository tableChecksumRepository;

    @Mock
    private MonitorProperties monitorProperties;

    private MonitorTask monitorTask;

    private static final String TABLE_NAME = "test_table";
    private static final String PRIMARY_KEY_NAME = "id";
    private static final Long PRIMARY_KEY_VALUE = 1L;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        // Configure MonitorProperties mock
        when(monitorProperties.getTableName()).thenReturn(TABLE_NAME);
        when(monitorProperties.getPrimaryKeyName()).thenReturn(PRIMARY_KEY_NAME);
        
        // Create MonitorTask instance with mocked dependencies
        monitorTask = new MonitorTask(
                monitorProperties,
                checksumService,
                auditRepository,
                monitorRepository,
                tableChecksumRepository
        );
    }

    @Test
    void execute_CallsProcessDeletedRowsAndProcessNewAndUpdatedRows() {
        // Create a spy of the monitorTask to verify protected method calls
        MonitorTask spyMonitorTask = spy(monitorTask);
        
        // Execute the method under test
        spyMonitorTask.execute();
        
        // Verify that both protected methods are called
        verify(spyMonitorTask).processDeletedRows();
        verify(spyMonitorTask).processNewAndUpdatedRows();
    }

    @Test
    void processDeletedRows_NoDeletedRows_DoesNothing() {
        // Configure monitorRepository to return null (no deleted rows)
        when(monitorRepository.findDeletedRows(TABLE_NAME, PRIMARY_KEY_NAME)).thenReturn(null);
        
        // Execute the method under test
        monitorTask.processDeletedRows();
        
        // Verify that auditRepository.insert is not called
        verify(auditRepository, never()).insert(any(Audit.class));
    }

    @Test
    void processDeletedRows_DeletedRowAlreadyProcessed_DoesNothing() {
        // Create a TableChecksum for a deleted row
        TableChecksum deletedRow = new TableChecksum();
        deletedRow.setPrimaryKey(PRIMARY_KEY_VALUE);
        
        // Configure monitorRepository to return the deleted row
        when(monitorRepository.findDeletedRows(TABLE_NAME, PRIMARY_KEY_NAME)).thenReturn(deletedRow);
        
        // Configure auditRepository to indicate the row is already deleted
        when(auditRepository.isAlreadyDeleted(TABLE_NAME, PRIMARY_KEY_VALUE)).thenReturn(true);
        
        // Execute the method under test
        monitorTask.processDeletedRows();
        
        // Verify that auditRepository.insert is not called
        verify(auditRepository, never()).insert(any(Audit.class));
    }

    @Test
    void processDeletedRows_NewDeletedRow_InsertsAuditRecord() {
        // Create a TableChecksum for a deleted row
        TableChecksum deletedRow = new TableChecksum();
        deletedRow.setPrimaryKey(PRIMARY_KEY_VALUE);
        
        // Configure monitorRepository to return the deleted row
        when(monitorRepository.findDeletedRows(TABLE_NAME, PRIMARY_KEY_NAME)).thenReturn(deletedRow);
        
        // Configure auditRepository to indicate the row is not already deleted
        when(auditRepository.isAlreadyDeleted(TABLE_NAME, PRIMARY_KEY_VALUE)).thenReturn(false);
        
        // Execute the method under test
        monitorTask.processDeletedRows();
        
        // Capture and verify the Audit object passed to auditRepository.insert
        ArgumentCaptor<Audit> auditCaptor = ArgumentCaptor.forClass(Audit.class);
        verify(auditRepository).insert(auditCaptor.capture());
        
        Audit capturedAudit = auditCaptor.getValue();
        assertEquals(TABLE_NAME, capturedAudit.getTableName());
        assertEquals(PRIMARY_KEY_VALUE, capturedAudit.getPrimaryKey());
        assertEquals(ChangeType.DELETE, capturedAudit.getChangeType());
        assertNotNull(capturedAudit.getChangeDate());
    }

    @Test
    void processNewAndUpdatedRows_EmptyTable_DoesNothing() {
        // Configure monitorRepository to return an empty list
        when(monitorRepository.findAll(TABLE_NAME, PRIMARY_KEY_NAME)).thenReturn(new ArrayList<>());
        
        // Execute the method under test
        monitorTask.processNewAndUpdatedRows();
        
        // Verify that no interactions with checksumService, tableChecksumRepository, or auditRepository occur
        verify(checksumService, never()).calculate(any());
        verify(tableChecksumRepository, never()).insert(any());
        verify(tableChecksumRepository, never()).update(any());
        verify(auditRepository, never()).insert(any());
    }

    @Test
    void processNewAndUpdatedRows_NewRow_InsertsChecksumAndAuditRecord() {
        // Create a row map
        Map<String, Object> row = new HashMap<>();
        row.put(PRIMARY_KEY_NAME, PRIMARY_KEY_VALUE);
        List<Map<String, Object>> rows = List.of(row);
        
        // Configure monitorRepository to return the row
        when(monitorRepository.findAll(TABLE_NAME, PRIMARY_KEY_NAME)).thenReturn(rows);
        
        // Configure checksumService to return a checksum
        long checksum = 12345L;
        when(checksumService.calculate(row)).thenReturn(checksum);
        
        // Configure tableChecksumRepository to indicate the row is new (not found)
        when(tableChecksumRepository.findByTableNameAndPrimaryKey(TABLE_NAME, PRIMARY_KEY_VALUE)).thenReturn(null);
        
        // Execute the method under test
        monitorTask.processNewAndUpdatedRows();
        
        // Capture and verify the TableChecksum object passed to tableChecksumRepository.insert
        ArgumentCaptor<TableChecksum> tableChecksumCaptor = ArgumentCaptor.forClass(TableChecksum.class);
        verify(tableChecksumRepository).insert(tableChecksumCaptor.capture());
        
        TableChecksum capturedTableChecksum = tableChecksumCaptor.getValue();
        assertEquals(TABLE_NAME, capturedTableChecksum.getTableName());
        assertEquals(PRIMARY_KEY_VALUE, capturedTableChecksum.getPrimaryKey());
        assertEquals(checksum, capturedTableChecksum.getCrc32());
        
        // Capture and verify the Audit object passed to auditRepository.insert
        ArgumentCaptor<Audit> auditCaptor = ArgumentCaptor.forClass(Audit.class);
        verify(auditRepository).insert(auditCaptor.capture());
        
        Audit capturedAudit = auditCaptor.getValue();
        assertEquals(TABLE_NAME, capturedAudit.getTableName());
        assertEquals(PRIMARY_KEY_VALUE, capturedAudit.getPrimaryKey());
        assertEquals(ChangeType.INSERT, capturedAudit.getChangeType());
        assertNotNull(capturedAudit.getChangeDate());
    }

    @Test
    void processNewAndUpdatedRows_UpdatedRow_UpdatesChecksumAndInsertsAuditRecord() {
        // Create a row map
        Map<String, Object> row = new HashMap<>();
        row.put(PRIMARY_KEY_NAME, PRIMARY_KEY_VALUE);
        List<Map<String, Object>> rows = List.of(row);
        
        // Configure monitorRepository to return the row
        when(monitorRepository.findAll(TABLE_NAME, PRIMARY_KEY_NAME)).thenReturn(rows);
        
        // Configure checksumService to return a new checksum
        long newChecksum = 67890L;
        when(checksumService.calculate(row)).thenReturn(newChecksum);
        
        // Create an existing TableChecksum with a different checksum
        TableChecksum existingTableChecksum = new TableChecksum();
        existingTableChecksum.setTableName(TABLE_NAME);
        existingTableChecksum.setPrimaryKey(PRIMARY_KEY_VALUE);
        existingTableChecksum.setCrc32(12345L); // Different from the new checksum
        
        // Configure tableChecksumRepository to return the existing TableChecksum
        when(tableChecksumRepository.findByTableNameAndPrimaryKey(TABLE_NAME, PRIMARY_KEY_VALUE))
                .thenReturn(existingTableChecksum);
        
        // Execute the method under test
        monitorTask.processNewAndUpdatedRows();
        
        // Verify that tableChecksumRepository.update is called with the updated TableChecksum
        ArgumentCaptor<TableChecksum> tableChecksumCaptor = ArgumentCaptor.forClass(TableChecksum.class);
        verify(tableChecksumRepository).update(tableChecksumCaptor.capture());
        
        TableChecksum capturedTableChecksum = tableChecksumCaptor.getValue();
        assertEquals(TABLE_NAME, capturedTableChecksum.getTableName());
        assertEquals(PRIMARY_KEY_VALUE, capturedTableChecksum.getPrimaryKey());
        assertEquals(newChecksum, capturedTableChecksum.getCrc32());
        
        // Capture and verify the Audit object passed to auditRepository.insert
        ArgumentCaptor<Audit> auditCaptor = ArgumentCaptor.forClass(Audit.class);
        verify(auditRepository).insert(auditCaptor.capture());
        
        Audit capturedAudit = auditCaptor.getValue();
        assertEquals(TABLE_NAME, capturedAudit.getTableName());
        assertEquals(PRIMARY_KEY_VALUE, capturedAudit.getPrimaryKey());
        assertEquals(ChangeType.UPDATE, capturedAudit.getChangeType());
        assertNotNull(capturedAudit.getChangeDate());
    }

    @Test
    void processNewAndUpdatedRows_UnchangedRow_DoesNotUpdateChecksumOrInsertAuditRecord() {
        // Create a row map
        Map<String, Object> row = new HashMap<>();
        row.put(PRIMARY_KEY_NAME, PRIMARY_KEY_VALUE);
        List<Map<String, Object>> rows = List.of(row);
        
        // Configure monitorRepository to return the row
        when(monitorRepository.findAll(TABLE_NAME, PRIMARY_KEY_NAME)).thenReturn(rows);
        
        // Configure checksumService to return a checksum
        long checksum = 12345L;
        when(checksumService.calculate(row)).thenReturn(checksum);
        
        // Create an existing TableChecksum with the same checksum
        TableChecksum existingTableChecksum = new TableChecksum();
        existingTableChecksum.setTableName(TABLE_NAME);
        existingTableChecksum.setPrimaryKey(PRIMARY_KEY_VALUE);
        existingTableChecksum.setCrc32(checksum); // Same as the calculated checksum
        
        // Configure tableChecksumRepository to return the existing TableChecksum
        when(tableChecksumRepository.findByTableNameAndPrimaryKey(TABLE_NAME, PRIMARY_KEY_VALUE))
                .thenReturn(existingTableChecksum);
        
        // Execute the method under test
        monitorTask.processNewAndUpdatedRows();
        
        // Verify that tableChecksumRepository.update and auditRepository.insert are not called
        verify(tableChecksumRepository, never()).update(any());
        verify(auditRepository, never()).insert(any());
    }

    @Test
    void processNewAndUpdatedRows_MultipleRows_ProcessesEachRow() {
        // Create row maps
        Map<String, Object> row1 = new HashMap<>();
        row1.put(PRIMARY_KEY_NAME, 1L);
        
        Map<String, Object> row2 = new HashMap<>();
        row2.put(PRIMARY_KEY_NAME, 2L);
        
        Map<String, Object> row3 = new HashMap<>();
        row3.put(PRIMARY_KEY_NAME, 3L);
        
        List<Map<String, Object>> rows = List.of(row1, row2, row3);
        
        // Configure monitorRepository to return the rows
        when(monitorRepository.findAll(TABLE_NAME, PRIMARY_KEY_NAME)).thenReturn(rows);
        
        // Configure checksumService to return different checksums for each row
        when(checksumService.calculate(row1)).thenReturn(1000L);
        when(checksumService.calculate(row2)).thenReturn(2000L);
        when(checksumService.calculate(row3)).thenReturn(3000L);
        
        // Configure tableChecksumRepository for each row:
        // row1: new row (null)
        // row2: updated row (different checksum)
        // row3: unchanged row (same checksum)
        when(tableChecksumRepository.findByTableNameAndPrimaryKey(TABLE_NAME, 1L)).thenReturn(null);
        
        TableChecksum existingTableChecksum2 = new TableChecksum();
        existingTableChecksum2.setTableName(TABLE_NAME);
        existingTableChecksum2.setPrimaryKey(2L);
        existingTableChecksum2.setCrc32(1000L); // Different from the calculated checksum (2000L)
        when(tableChecksumRepository.findByTableNameAndPrimaryKey(TABLE_NAME, 2L)).thenReturn(existingTableChecksum2);
        
        TableChecksum existingTableChecksum3 = new TableChecksum();
        existingTableChecksum3.setTableName(TABLE_NAME);
        existingTableChecksum3.setPrimaryKey(3L);
        existingTableChecksum3.setCrc32(3000L); // Same as the calculated checksum
        when(tableChecksumRepository.findByTableNameAndPrimaryKey(TABLE_NAME, 3L)).thenReturn(existingTableChecksum3);
        
        // Execute the method under test
        monitorTask.processNewAndUpdatedRows();
        
        // Verify interactions for row1 (new row)
        verify(tableChecksumRepository).insert(argThat(tc -> tc.getPrimaryKey() == 1L));
        verify(auditRepository).insert(argThat(a -> a.getPrimaryKey() == 1L && a.getChangeType() == ChangeType.INSERT));
        
        // Verify interactions for row2 (updated row)
        verify(tableChecksumRepository).update(argThat(tc -> tc.getPrimaryKey() == 2L));
        verify(auditRepository).insert(argThat(a -> a.getPrimaryKey() == 2L && a.getChangeType() == ChangeType.UPDATE));
        
        // Verify no interactions for row3 (unchanged row)
        verify(tableChecksumRepository, never()).update(argThat(tc -> tc.getPrimaryKey() == 3L));
        verify(auditRepository, never()).insert(argThat(a -> a.getPrimaryKey() == 3L));
    }
}