package io.tarantool.driver.proxy;

import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.metadata.TarantoolIndexPartMetadata;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Convert index query to list of arguments for proxy select
 *
 * @author Sergey Volgin
 */
public final class ProxySelectArgumentsConverter {
    private static final Logger log = LoggerFactory.getLogger(ProxySelectArgumentsConverter.class);

    public static final String EQ = "=";
    public static final String GE = ">=";
    public static final String GT = ">";
    public static final String LE = "<=";
    public static final String LT = "<";

    private ProxySelectArgumentsConverter() {
    }

    public static List<?> fromIndexQuery(TarantoolIndexQuery indexQuery,
                                         List<TarantoolIndexPartMetadata> indexParts,
                                         TarantoolSpaceMetadata spaceMetadata) {
        List<Object> operations = null;

        if (!indexQuery.getKeyValues().isEmpty()) {
            operations = new ArrayList<>();
            String operator = null;

            switch (indexQuery.getIteratorType()) {
                case ITER_EQ:
                    operator = EQ;
                    break;
                case ITER_GE:
                    operator = GE;
                    break;
                case ITER_GT:
                    operator = GT;
                    break;
                case ITER_LE:
                    operator = LE;
                    break;
                case ITER_LT:
                    operator = LT;
                    break;
                default:
                    log.warn("Iterator {} does not support for proxy select.", indexQuery.getIteratorType());
            }

            if (operator != null) {
                int fieldIndex = 0;
                for (Object keyValue : indexQuery.getKeyValues()) {
                    TarantoolIndexPartMetadata indexPart = indexParts.get(fieldIndex);
                    Optional<String> fieldName = spaceMetadata.getFieldNameByPosition(indexPart.getFieldIndex());
                    if (!fieldName.isPresent()) {
                        throw new TarantoolClientException("Index part has no name");
                    }
                    operations.add(Arrays.asList(operator, fieldName.get(), keyValue));
                    fieldIndex++;
                }
            }
        }

        return operations;
    }
}
