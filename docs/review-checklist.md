# Code review checklist for java projects

- Added changes to CHANGELOG
- No changes in files not related to the task
  (except for a simple refactoring that obviously brings benefits and does not complicate the review)
- No obvious flaky tests
- Variable, method, and class names convey the subject
- No console print statements
- No unnecessary comments
- New features have tests
- Bugfixes have tests based on reproducers
- StringBuilder or StringBuffer is used to perform multiple concatenations on strings (instead of '+')
- For many choices `switch case` is used instead of multiple if-else conditions
- Used checked exceptions for recoverable operations and runtime exceptions for programming errors
- Code is DRY (see Do not Repeat Yourself principle)
- Code does not break SOLID principles
- Methods grouped by functionality rather than by randomly, scope or accessibility. For example, a private class method
  can be in between two public instance methods. The goal is to make reading and understanding the code easier.
- Ensure override hashCode when overriding equals
- Used @deprecated on method/variable names that are not meant for future use