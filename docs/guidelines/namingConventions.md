# 📘 Coding Standards


## Index

- [Naming Conventions](#naming-conventions)
  - [Introduction](#introduction)
  - [General Principles](#general-principles)
    - [Clarity and Readability Over Brevity](#clarity-and-readability-over-brevity)
    - [Consistency Within the Project](#consistency-within-the-project)
    - [Use of Descriptive Names, Avoiding Generic Names When Possible](#use-of-descriptive-names-avoiding-generic-names-when-possible)
  - [Package Names](#package-names)
    - [Short, Lowercase, and One-word Names](#short-lowercase-and-one-word-names)
    - [Avoidance of Common Names Like "util" or "helper"](#avoidance-of-common-names-like-util-or-helper)
    - [Examples and Exceptions](#examples-and-exceptions)
  - [Variable Names](#variable-names)
    - [Short Yet Descriptive Names](#short-yet-descriptive-names)
    - [CamelCase for Exportable Variables and camelCase for Internal Variables](#camelcase-for-exportable-variables-and-camelcase-for-internal-variables)
    - [Common Idioms](#common-idioms)
  - [Function Names](#function-names)
    - [Use of Descriptive Verbs and Nouns](#use-of-descriptive-verbs-and-nouns)
    - [Naming Conventions for Constructors, Getters, and Setters](#naming-conventions-for-constructors-getters-and-setters)
    - [Error Handling Functions and Their Naming Patterns](#error-handling-functions-and-their-naming-patterns)
  - [Interface Names](#interface-names)
    - [Single Method Interfaces with "-er" Suffix](#single-method-interfaces-with--er-suffix)
    - [Use of Descriptive Names for More Complex Interfaces](#use-of-descriptive-names-for-more-complex-interfaces)
  - [Type Names](#type-names)
    - [Avoidance of Redundant or Tautological Names](#avoidance-of-redundant-or-tautological-names)
    - [Use of Clear and Specific Names for Custom Types](#use-of-clear-and-specific-names-for-custom-types)
  - [Commenting](#commenting)
    - [Best Practices for Writing Clear and Helpful Comments](#best-practices-for-writing-clear-and-helpful-comments)
  - [Error Handling](#error-handling)
    - [Effective Use of Go's Error Handling Paradigm](#effective-use-of-gos-error-handling-paradigm)
    - [Patterns for Error Creation, Propagation, and Checking](#patterns-for-error-creation-propagation-and-checking)
  - [Concurrency](#concurrency)
    - [Best Practices for Using Goroutines and Channels](#best-practices-for-using-goroutines-and-channels)
    - [Patterns for Avoiding Common Concurrency Pitfalls](#patterns-for-avoiding-common-concurrency-pitfalls)
  - [Testing](#testing)
    - [Writing Effective Unit Tests Using the "testing" Package](#writing-effective-unit-tests-using-the-testing-package)
    - [Use of Table-Driven Tests for Comprehensive Coverage](#use-of-table-driven-tests-for-comprehensive-coverage)
  - [Dependency Management](#dependency-management)
    - [Use of Modules for Managing Dependencies](#use-of-modules-for-managing-dependencies)
    - [Strategies for Keeping Dependencies Up to Date and Secure](#strategies-for-keeping-dependencies-up-to-date-and-secure)

## Naming Conventions

### Introduction

The Teranode BSV implementation follows the Go programming language's naming conventions and best practices. These conventions are based on the official Go documentation, effective Go, and the Go community's accepted practices. To know more about them, please check:

- https://go.dev/doc/effective_go
- https://go.dev/talks/2014/names.slide#1

The naming conventions and best practices outlined in this document provide a summary of these coding and naming best practices, together with additional guidelines specific to the Teranode BSV implementation.

### General Principles

#### Clarity and Readability Over Brevity

- Goal: Clear and readable code.
- Descriptive names > succinct names to avoid ambiguity.
- Longer, explicit names preferred over cryptic abbreviations.
- Example: Use `bestBlockchainBlockHeader` instead of `bestBlkHdr` for clarity.
-
#### Consistency Within the Project

- Consistency maintains a coherent codebase.
- Applies to naming, formatting, commenting, and code structure.
- Helps developers quickly understand and navigate new code sections.

#### Use of Descriptive Names, Avoiding Generic Names When Possible

- Use descriptive and specific names for clarity on role and usage.
- Descriptive names serve as self-documenting elements.
- Avoid generic names (e.g., `data`, `info`, `manager`) that lack insight.
- Choose names reflecting the entity's purpose (e.g., `miningCandidate`, `coinbaseValue`).
- Enhances code intuitiveness, readability, and maintainability.

### Package Names

#### Short, Lowercase, and One-word Names

- Package names: concise, lowercase, single word if possible.
- Improves readability and avoids variable name conflicts.
- Examples: Use `net` (not `networkOperations`), `time` (not `timeUtils`).

#### Avoidance of Common Names Like "util" or "helper"

- Avoid generic package names like `util`, `utils`, `helper`.
- Generic names lead to unclear purpose and mixed contents.
- Name packages after their function or representation, e.g., `blockassembly` for Block Assembly.

#### Examples and Exceptions

- **Good Package Names**: `http`, `os`, `json` – These names are short, descriptive, and specific to their functionality.
- **Avoid**: `utilities`, `common`, `shared` – These are generic and do not convey the package's contents or purpose.

**Exceptions** may arise when a package is designed to extend or wrap standard library functionality with more specific features. In such cases, appending a descriptive term to a standard library package name can be acceptable, e.g., `httputil` or `ioutil`, though the latter is deprecated in favor of more descriptive package names like `io` and `os`.


### Variable Names

#### Short Yet Descriptive Names

- Choose brief, descriptive names for variables.
- Aim to convey purpose without sacrificing readability.
- Avoid overly long names.

#### CamelCase for Exportable Variables and camelCase for Internal Variables

- **Exportable Variables**: Use CamelCase (capitalizing the first letter) for variables that need to be accessible outside the package, e.g., `CustomerID`.
- **Internal Variables**: Use camelCase (starting with a lowercase letter) for variables only used within the package, e.g., `localTime`.

#### Common Idioms

- **Loop Indices**: Use short names like `i`, `j`, `k` for loops.
- **Errors**: Use `err` to represent errors.
- **Temporary Variables**: Short names like `tmp` or `temp` are acceptable for temporary or insignificant variables.

### Function Names

#### Use of Descriptive Verbs and Nouns

- Use descriptive verbs and nouns in function names.
- Clearly indicate function action and subject.
- Examples: `CalculateTotal`, `ReadFile`, `PrintMessage`.

#### Naming Conventions for Constructors, Getters, and Setters

- **Constructors**: Prefixed with `New` or `Make` indicating creation, e.g., `NewUser` or `MakeConnection`.
- **Getters**: No prefix; use the property name directly, avoiding the `Get` prefix, e.g., `Name()` instead of `GetName()`.
- **Setters**: Prefixed with `Set` followed by the property name, e.g., `SetName(value)`.

#### Error Handling Functions and Their Naming Patterns

- Name functions that return errors with action verbs: `Open`, `Read`, `Write`.
  - It's clear from the context that an error can be returned. Context should suggest an error can be returned, e.g., `os.Open`.
- Avoid "Error" in names; return type already implies error possibility.

### Interface Names

#### Single Method Interfaces with "-er" Suffix

- For interfaces with a single method, use a name ending in "-er" to describe the action performed by the method, such as `Reader`, `Writer`, or `Closer`.

#### Use of Descriptive Names for More Complex Interfaces

- For interfaces with multiple methods, choose descriptive names that capture the overall functionality or role of the interface, rather than following the "-er" suffix rule.
  - For example, `FileSystem` for an interface that encapsulates various file system operations, or `DatabaseConnector` for an interface managing database connections.

### Type Names

#### Avoidance of Redundant or Tautological Names

- Avoid names that repeat the package name or provide no additional information about the type.
  - For instance, instead of `http.HttpClient`, simply use `http.Client` to prevent redundancy.

#### Use of Clear and Specific Names for Custom Types

- Choose names that clearly and specifically describe what the custom type represents or does, ensuring they are intuitive and meaningful.
  - For example, `Block` for a type representing block information, or `SubtreeProcessor` for a type that processes subtrees.

### Commenting

#### Best Practices for Writing Clear and Helpful Comments

- **Descriptive Comments**: Write comments that explain the "why" behind code logic, not just the "what". This helps readers understand the purpose and reasoning.
- **Package Comments**: Start with a package comment in a file named `doc.go` that describes the package's purpose and provides an overview of its functionality.
- **Function Comments**: Begin with the function name and describe what the function does, its parameters, return values, and any side effects.
- **Avoid Redundant Comments**: Don't state the obvious. Focus on providing additional context or information not readily apparent from the code itself.


### Error Handling

#### Effective Use of Go's Error Handling Paradigm

- **Explicit Error Checking**: Always check for errors by comparing the returned error to `nil`. Handle the error appropriately where it occurs.

```go
if err != nil {
// Handle error
}
```

#### Patterns for Error Creation, Propagation, and Checking

**- TBD**

- **Propagating Errors**: When an error occurs, return it to the caller instead of handling it unless you can resolve it or it's critical to continue execution.

```go
if err != nil {
return err
}
```

- **Custom Error Types**: For more complex error handling, define custom error types that implement the `error` interface.

**- TBD**


### Concurrency

Go's concurrency model, centered around goroutines and channels, enables efficient parallel execution. Here are best practices and patterns for its effective use.

#### Best Practices for Using Goroutines and Channels

- **Start Simple**: Begin with a simple design. Use goroutines for asynchronous tasks and channels for communication.
- **Avoid Shared State**: Prefer channels to share data between goroutines instead of shared memory to avoid race conditions.
- **Buffered Channels**: Use buffered channels when you know the capacity ahead of time or to limit the number of goroutines running concurrently.

#### Patterns for Avoiding Common Concurrency Pitfalls

- **Worker Pools**: Implement worker pools to control the number of goroutines performing work simultaneously, preventing excessive resource consumption.
- **Select Statement**: Use the `select` statement to wait on multiple channel operations, enhancing control over channel communication.
- **Context Package**: Use the `context` package to manage and cancel goroutines, providing a way to control goroutine lifecycles and prevent leaks.

By following these guidelines, you can leverage Go's concurrency features effectively, creating programs that are scalable, efficient, and robust.

### Testing

#### Writing Effective Unit Tests Using the "testing" Package

- **Basic Structure**: Utilize the `testing.T` type to create tests. Each test function should be named `TestXxx`, where `Xxx` does not start with a lowercase letter.

```go
func TestXxx(t *testing.T) {
// Test code here
}
```

- **Assert Conditions**: Use `if` statements to test conditions within your tests. Use `t.Error` or `t.Errorf` to report failures.

```go
if got != want {
t.Errorf("got %q, want %q", got, want)
}
```

#### Use of Table-Driven Tests for Comprehensive Coverage

- Implement table-driven tests by defining test cases as structs in a slice, iterating over them in a single test function.

```go
var tests = []struct {
input string
want  string
}{
{"input1", "want1"},
{"input2", "want2"},
// More test cases
}

for _, tt := range tests {
testname := fmt.Sprintf("%s,%s", tt.input, tt.want)
t.Run(testname, func(t *testing.T) {
ans := MyFunc(tt.input)
if ans != tt.want {
t.Errorf("got %s, want %s", ans, tt.want)
}
})
}
```

### Dependency Management

#### Use of Modules for Managing Dependencies

- **Modules**: Adopt Go modules for dependency management by initializing a new module via `go mod init <module name>`, which creates a `go.mod` file to track your project's dependencies.

    ```shell
    go mod init mymodule
    ```

- **Dependency Tracking**: The `go.mod` file lists the specific versions of external packages your project depends on, ensuring consistent builds.

#### Strategies for Keeping Dependencies Up to Date and Secure

- **Regular Updates**: Regularly run `go get -u` to update your dependencies to their latest minor or patch versions.

    ```shell
    go get -u
    ```

- **Vulnerability Checks**: Use tools like `go list -m all | go get -u` and `go mod tidy` to find and fix known vulnerabilities in dependencies, and to clean up unused dependencies.

    ```shell
    go list -m all | go get -u
    go mod tidy
    ```

- **Version Pinning**: Pin dependencies to specific versions or ranges in your `go.mod` file to avoid breaking changes and ensure reproducibility.
