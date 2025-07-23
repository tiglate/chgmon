package ludo.mentis.aciem.chgmon.repos;

import ludo.mentis.aciem.chgmon.model.TableChecksum;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Objects;

@Repository
public class TableChecksumRepositoryImpl implements TableChecksumRepository {

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public TableChecksumRepositoryImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Writes a checksum record to the tb_table_checksum table.
     *
     * @param tableChecksum the TableChecksum object containing the data to be written
     * @return the ID of the inserted record
     * @throws IllegalArgumentException if tableChecksum is null or has null required fields
     */
    @Override
    public Integer insert(TableChecksum tableChecksum) {
        if (tableChecksum == null) {
            throw new IllegalArgumentException("TableChecksum cannot be null");
        }

        if (tableChecksum.getTableName() == null || tableChecksum.getTableName().trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }

        if (tableChecksum.getPrimaryKey() == null) {
            throw new IllegalArgumentException("Primary key cannot be null");
        }

        if (tableChecksum.getCrc32() == null) {
            throw new IllegalArgumentException("CRC32 cannot be null");
        }

        KeyHolder keyHolder = new GeneratedKeyHolder();

        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO tb_table_checksum (table_name, primary_key, crc32) VALUES (?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS
            );
            ps.setString(1, tableChecksum.getTableName());
            ps.setLong(2, tableChecksum.getPrimaryKey());
            ps.setLong(3, tableChecksum.getCrc32());
            return ps;
        }, keyHolder);

        return Objects.requireNonNull(keyHolder.getKey()).intValue();
    }

    @Override
    public TableChecksum findByTableNameAndPrimaryKey(String tableName, Long primaryKey) {
        if (primaryKey == null) {
            throw new IllegalArgumentException("Primary key cannot be null");
        }

        String sql = "SELECT id_table_checksum, table_name, primary_key, crc32 FROM tb_table_checksum WHERE table_name = ? AND primary_key = ?";
        
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                TableChecksum tableChecksum = new TableChecksum();
                tableChecksum.setId(rs.getInt("id_table_checksum"));
                tableChecksum.setTableName(rs.getString("table_name"));
                tableChecksum.setPrimaryKey(rs.getLong("primary_key"));
                tableChecksum.setCrc32(rs.getLong("crc32"));
                return tableChecksum;
            }, tableName, primaryKey);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null; // Return null if no record is found
        }
    }
    
    /**
     * Updates an existing checksum record in the tb_table_checksum table.
     *
     * @param tableChecksum the TableChecksum object containing the updated data
     * @return true if the update was successful, false otherwise
     * @throws IllegalArgumentException if tableChecksum is null or has null required fields
     */
    @Override
    public boolean update(TableChecksum tableChecksum) {
        if (tableChecksum == null) {
            throw new IllegalArgumentException("TableChecksum cannot be null");
        }

        if (tableChecksum.getId() == null) {
            throw new IllegalArgumentException("TableChecksum ID cannot be null");
        }

        if (tableChecksum.getTableName() == null || tableChecksum.getTableName().trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }

        if (tableChecksum.getPrimaryKey() == null) {
            throw new IllegalArgumentException("Primary key cannot be null");
        }

        if (tableChecksum.getCrc32() == null) {
            throw new IllegalArgumentException("CRC32 cannot be null");
        }

        String sql = "UPDATE tb_table_checksum SET table_name = ?, primary_key = ?, crc32 = ? WHERE id_table_checksum = ?";
        
        int rowsAffected = jdbcTemplate.update(sql, 
            tableChecksum.getTableName(),
            tableChecksum.getPrimaryKey(),
            tableChecksum.getCrc32(),
            tableChecksum.getId()
        );
        
        return rowsAffected > 0;
    }
}
