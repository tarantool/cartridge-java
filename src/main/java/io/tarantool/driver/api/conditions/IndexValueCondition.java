package io.tarantool.driver.api.conditions;

import java.util.List;

/**
 * Tuple filtering condition which accepts index key parts values
 *
 * @author Alexey Kuzin
 */
public class IndexValueCondition extends BaseCondition {

    private final List<Object> indexValues;

    /**
     * Basic constructor
     *
     * @param operator the filtering operator
     * @param field the filtering index
     * @param indexValues the index parts values
     */
    public IndexValueCondition(Operator operator, FieldIdentifier<?, ?> field, List<Object> indexValues) {
        super(operator, field);
        this.indexValues = indexValues;
    }

    @Override
    public List<Object> value() {
        return indexValues;
    }

//    @Override
//    public List<Object> toList(TarantoolSpaceMetadataOperations spaceMetadataOperations) {
//        List<Object> result = new ArrayList<>();
//
//        TarantoolIndexMetadata indexMetadata;
//        if (field instanceof NamedIndex) {
//            indexMetadata = spaceMetadataOperations.getIndexByName((String) field.toIdentifier());
//        } else {
//            indexMetadata = spaceMetadataOperations.getIndexById((Integer) field.toIdentifier());
//        }
//
//        for (int i = 0; i < value().size(); i++) {
//            int position = indexMetadata.getIndexParts().get(i).getFieldIndex();
//            Optional<TarantoolFieldMetadata> fieldMetadata =
//                    spaceMetadataOperations.getSpaceMetadata().getFieldByPosition(position);
//
//            if (!fieldMetadata.isPresent()) {
//                throw new TarantoolFieldNotFoundException(position, spaceMetadataOperations.getSpaceMetadata());
//            }
//
//            result.add(Arrays.asList(operator.getCode(), fieldMetadata.get().getFieldName(), value().get(i)));
//        }
//
//        return result;
//    }
}
