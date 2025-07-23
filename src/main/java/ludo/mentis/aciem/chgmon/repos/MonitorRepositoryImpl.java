package ludo.mentis.aciem.chgmon.repos;

import ludo.mentis.aciem.chgmon.model.TableChecksum;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class MonitorRepositoryImpl implements MonitorRepository {

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public MonitorRepositoryImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Retrieves all rows from a specified table.
     *
     * @param tableName    the name of the table to query
     * @param primaryKeyName the name of the primary key column
     * @return a list of maps, where each map represents a row with column names as keys and column values as values
     * @throws IllegalArgumentException if tableName or primaryKeyName is null or empty
     */
    @Override
    public List<Map<String, Object>> findAll(String tableName, String primaryKeyName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        
        if (primaryKeyName == null || primaryKeyName.trim().isEmpty()) {
            throw new IllegalArgumentException("Primary key name cannot be null or empty");
        }

        var sql = String.format("SELECT * FROM %s ORDER BY %s", tableName, primaryKeyName);
        
        return jdbcTemplate.queryForList(sql);
    }

    @Override
    public TableChecksum findDeletedRows(String tableName, String primaryKeyName) {
        var sql = String.format("SELECT * FROM tb_table_checksum WHERE table_name = '%s' AND primary_key NOT IN (SELECT %s FROM %s)", tableName, primaryKeyName, tableName);
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                var tableChecksum = new TableChecksum();
                tableChecksum.setId(rs.getInt("id_table_checksum"));
                tableChecksum.setTableName(rs.getString("table_name"));
                tableChecksum.setPrimaryKey(rs.getLong("primary_key"));
                tableChecksum.setCrc32(rs.getLong("crc32"));
                return tableChecksum;
            });
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null; // Return null if no record is found
        }
    }
}
