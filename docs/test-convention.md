Tests names should correspond the pattern:

**test_[methodName/scenarioName]_should[Action]_if[Condition]**

**[methodName/scenarioName]_ - is optional.**
**if[Condition] - is optional. You can omit this if the execution is straightforward.**

Also, it's nice to include additional comments of the following format:
```   
    //given
    [some code that defines conditions]
    //when
    [some code that executes the test case]
    //then
    [some code with assertions]
```

Example:
If we have method like this:

```java   
    public String hello(String name) {
        return "Hello, " + name;
    }
```

Test for this method can look like:
```java
    @Test
    public void test_hello_shouldReturnHelloWithName() {
        //given
        String name = "John";
        
        //when
        String welcome = hello(name);
        
        //then
        assertThat(welcome).isEqualTo("Hello, John");
    }
```

If we have method like this:
```java
    public String helloWithCondition(@Nullable String name) {
        if (name == null) {
            return "Hello Stranger";
        }
        return "Hello, " + name;
    }
```

Test for this method can be like this:
```java
    @Test
    public void test_helloWithCondition_shouldReturnHelloStranger_ifNameIsNull() {
        //given
        String name = null;

        //when
        String welcome = helloWithCondition(name);

        //then
        assertThat(welcome).isEqualTo("Hello, Stranger");
    }
```

It's not always possible to use the above pattern for each test method name.
When the description is too large then test it's good to describe test scenario in comment.
Also consider this points:

- Name of test method must not violate linter rules and generate compiler warnings (must not contain special characters, be longer than 120 chars etc).
- These points of test case must be readable and simple:
    - test object (class, method)
    - action (input)
    - expected reaction of system (output)
- Do not perform a total refactoring of existing codebase.
  Just leave the code better than it was after making changes.