package ludo.mentis.aciem.chgmon.repos;

import ludo.mentis.aciem.chgmon.model.Audit;

public interface AuditRepository {

    Integer insert(Audit audit);

    boolean isAlreadyDeleted(String tableName, Long primaryKey);
}