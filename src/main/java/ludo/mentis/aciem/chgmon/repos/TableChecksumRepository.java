package ludo.mentis.aciem.chgmon.repos;

import ludo.mentis.aciem.chgmon.model.TableChecksum;

public interface TableChecksumRepository {

    Integer insert(TableChecksum tableChecksum);

    TableChecksum findByTableNameAndPrimaryKey(String tableName, Long primaryKey);
    
    boolean update(TableChecksum tableChecksum);
}
