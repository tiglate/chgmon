package ludo.mentis.aciem.chgmon.service;

import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.zip.CRC32;

@Service
public class ChecksumServiceImpl implements ChecksumService {

    @Override
    public long calculate(Map<String, Object> row) {
        var crc32 = new CRC32();
        for (var column : row.entrySet()) {
            crc32.update(column.getKey().getBytes());
            if (column.getValue() != null) {
                crc32.update(column.getValue().toString().getBytes());
            }
        }
        return crc32.getValue();
    }
}
