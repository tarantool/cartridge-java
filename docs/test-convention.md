Tests names should be correspond the next pattern:

**test_[methodName/scenarioName]_should[Action]_if[Condition]**

**[methodName/scenarioName]_ - is optional.**
**if[Condition] - is optional. You can omit this if the execution is straightforward.**

Also, inside the body of the test there should be comments of the following format:
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

Test for this method:
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

It's good when this pattern can be included into test method name, but it's not always possible.
When the description is too large then test case can contain additional info in comment.
Also consider this points:

- Name of test method must not violate linter rules and generate compiler warnings (must not contain special characters, be longer than 120 chars etc).
- These points of test case must be readable and simple:
    - test object (class, method)
    - action (input)
    - expected reaction of system (output)
- Do not perform a total refactoring of existing codebase.
  Just leave the code better than it was after making changes.