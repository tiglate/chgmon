package ludo.mentis.aciem.chgmon.task;

import ludo.mentis.aciem.chgmon.config.MonitorProperties;
import ludo.mentis.aciem.chgmon.model.Audit;
import ludo.mentis.aciem.chgmon.model.ChangeType;
import ludo.mentis.aciem.chgmon.model.TableChecksum;
import ludo.mentis.aciem.chgmon.repos.AuditRepository;
import ludo.mentis.aciem.chgmon.repos.MonitorRepository;
import ludo.mentis.aciem.chgmon.repos.TableChecksumRepository;
import ludo.mentis.aciem.chgmon.service.ChecksumService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class MonitorTask {

    private final ChecksumService checksumService;
    private final AuditRepository auditRepository;
    private final MonitorRepository monitorRepository;
    private final TableChecksumRepository tableChecksumRepository;
    private final String tableName;
    private final String primaryKeyName;
    private static final Logger logger = LoggerFactory.getLogger(MonitorTask.class);

    public MonitorTask(MonitorProperties monitorConfig,
                       ChecksumService checksumService,
                       AuditRepository auditRepository,
                       MonitorRepository monitorRepository,
                       TableChecksumRepository tableChecksumRepository) {
        this.tableName = monitorConfig.getTableName();
        this.primaryKeyName = monitorConfig.getPrimaryKeyName();
        this.checksumService = checksumService;
        this.auditRepository = auditRepository;
        this.monitorRepository = monitorRepository;
        this.tableChecksumRepository = tableChecksumRepository;
    }

    @Scheduled(cron = "${monitor.cron}")
    public void execute() {
        logger.debug("Executing monitor task for table: {}", tableName);
        processDeletedRows();
        processNewAndUpdatedRows();
        System.gc();
    }

    protected void processDeletedRows() {
        var deletedRows = monitorRepository.findDeletedRows(tableName, primaryKeyName);
        if (deletedRows != null) {
            if (auditRepository.isAlreadyDeleted(tableName, deletedRows.getPrimaryKey())) {
                logger.debug("Table: {}. Already deleted row: {}", tableName, deletedRows.getPrimaryKey());
                return;
            }

            var audit = new Audit();
            audit.setTableName(tableName);
            audit.setPrimaryKey(deletedRows.getPrimaryKey());
            audit.setChangeType(ChangeType.DELETE);
            audit.setChangeDate(LocalDateTime.now());
            auditRepository.insert(audit);
            logger.info("Table: {}. Deleted row: {}", tableName, deletedRows.getPrimaryKey());
        }
    }

    protected void processNewAndUpdatedRows() {
        var table = monitorRepository.findAll(tableName, primaryKeyName);

        for (var row : table) {
            var checksum = checksumService.calculate(row);

            var tableChecksum = tableChecksumRepository.findByTableNameAndPrimaryKey(tableName, (Long) row.get(primaryKeyName));
            if (tableChecksum == null) {
                tableChecksum = new TableChecksum();
                tableChecksum.setTableName(tableName);
                tableChecksum.setPrimaryKey((Long) row.get(primaryKeyName));
                tableChecksum.setCrc32(checksum);
                tableChecksumRepository.insert(tableChecksum);

                var audit = new Audit();
                audit.setTableName(tableName);
                audit.setPrimaryKey((Long) row.get(primaryKeyName));
                audit.setChangeType(ChangeType.INSERT);
                audit.setChangeDate(LocalDateTime.now());
                auditRepository.insert(audit);

                logger.info("Table: {}. Inserted row: {}", tableName, row.get(primaryKeyName));
            } else if (tableChecksum.getCrc32() != checksum) {
                tableChecksum.setCrc32(checksum);
                tableChecksumRepository.update(tableChecksum);

                var audit = new Audit();
                audit.setTableName(tableName);
                audit.setPrimaryKey((Long) row.get(primaryKeyName));
                audit.setChangeType(ChangeType.UPDATE);
                audit.setChangeDate(LocalDateTime.now());
                auditRepository.insert(audit);

                logger.info("Table: {}. Updated row: {}", tableName, row.get(primaryKeyName));
            } else {
                logger.debug("Table: {}. No changes for row: {}", tableName, row.get(primaryKeyName));
            }
        }
    }
}
