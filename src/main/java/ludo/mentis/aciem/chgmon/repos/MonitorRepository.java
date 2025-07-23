package ludo.mentis.aciem.chgmon.repos;

import ludo.mentis.aciem.chgmon.model.TableChecksum;

import java.util.List;
import java.util.Map;

public interface MonitorRepository {

    List<Map<String, Object>> findAll(String tableName, String primaryKeyName);

    TableChecksum findDeletedRows(String tableName, String primaryKeyName);
}
