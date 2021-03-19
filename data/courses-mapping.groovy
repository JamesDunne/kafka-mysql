import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.*

class CourseFilter implements Predicate<String, Map<String, Object>> {
  @Override
  boolean test(String k, Map<String, Object> v) {
    return true
  }
}

class CourseMap implements ValueMapperWithKey<String, Map<String, Object>, Map<String, Object>> {
  @Override
  Map<String, Object> apply(String k, Map<String, Object> v) {
    v.taughtBy = v.taughtBy.findAll { it -> it.personId != null }
    return v
  }
}

class PersonFilter implements Predicate<String, Map<String, Object>> {
  @Override
  boolean test(String k, Map<String, Object> v) {
    return true
  }
}

class PersonMap implements ValueMapperWithKey<String, Map<String, Object>, Map<String, Object>> {
  @Override
  Map<String, Object> apply(String k, Map<String, Object> v) {
    v.advisedBy = v.advisedBy.findAll { it -> it.personId != null }
    return v
  }
}
