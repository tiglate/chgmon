package ludo.mentis.aciem.chgmon.repos;

import ludo.mentis.aciem.chgmon.model.Audit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Objects;

@Repository
public class AuditRepositoryImpl implements AuditRepository {

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public AuditRepositoryImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    /**
     * Inserts an audit record into the tb_audit table.
     *
     * @param audit the Audit object containing the data to be inserted
     * @return the ID of the inserted record
     * @throws IllegalArgumentException if audit is null or has null required fields
     */
    @Override
    public Integer insert(Audit audit) {
        if (audit == null) {
            throw new IllegalArgumentException("Audit cannot be null");
        }
        
        if (audit.getTableName() == null || audit.getTableName().trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        
        if (audit.getPrimaryKey() == null) {
            throw new IllegalArgumentException("Primary key cannot be null");
        }
        
        if (audit.getChangeType() == null) {
            throw new IllegalArgumentException("Change type cannot be null");
        }
        
        KeyHolder keyHolder = new GeneratedKeyHolder();
        
        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO tb_audit (primary_key, table_name, change_type, change_date) VALUES (?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS
            );
            ps.setLong(1, audit.getPrimaryKey());
            ps.setString(2, audit.getTableName());
            ps.setString(3, audit.getChangeType().name());
            
            // If change_date is provided, use it; otherwise, let the database use its default (GETDATE())
            if (audit.getChangeDate() != null) {
                ps.setTimestamp(4, Timestamp.valueOf(audit.getChangeDate()));
            } else {
                ps.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
            }
            
            return ps;
        }, keyHolder);
        
        return Objects.requireNonNull(keyHolder.getKey()).intValue();
    }

    @Override
    public boolean isAlreadyDeleted(String tableName, Long primaryKey) {
        var sql = "SELECT COUNT(*) FROM tb_audit WHERE table_name = ? AND primary_key = ? AND change_type = 'DELETE'";
        var count = jdbcTemplate.queryForObject(sql, Integer.class, tableName, primaryKey);
        return count != null && count > 0;
    }
}
