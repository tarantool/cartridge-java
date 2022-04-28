# Code review checklist for java projects

## 'Must' rules
- Added changes to CHANGELOG
- No changes in files not related to the task
  (except for a simple refactoring that obviously brings benefits and does not complicate the review)
- No obvious flaky tests
- Variable, method, and class names convey the subject
- No console print statements
- No unnecessary comments
- New features have tests
- Bugfixes have tests based on reproducers

## 'Nice to have' rules
- Keep code DRY: any simple code part shouldn't be repeated more than two times
- Keep code SOLID
- Any complex code part should be enclosed in a corresponding method or a class
- Use RuntimeExceptions most of the time to not complicate the users' code and make  
  it more usable in the reactive programming patterns. All internally produced exceptions 
  must be classified and be instances of a TarantoolException class
- Group methods by functionality, scope or access. For example a private method should not 
  be defined between two public methods. 
- Used @deprecated on method/variable names that are not meant for future use
- For many choices `switch case` is used instead of multiple if-else conditions
- Use enums instead of string constants
- Ensure override hashCode when overriding equals. It's better to use generated hashCode/equals 
  by your IDE (I suggest IDEA for this purpose). Don't write any custom code here if not strictly
  necessary. Also, don't add these methods if the class is not supposed to be used as a hash key 
  or to be serialized. This will help to avoid the wrong usage.
