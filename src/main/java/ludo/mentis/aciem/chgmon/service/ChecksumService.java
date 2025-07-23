package ludo.mentis.aciem.chgmon.service;

import java.util.Map;

public interface ChecksumService {

    long calculate(Map<String, Object> row);
}
