# `did.query`

The `did.query` module provides the `Query` class, which is used to construct search queries for the database.

## `Query` Class

The `Query` class allows you to build complex search queries by combining simple search terms. You can create a query by specifying a field, an operation, and one or more parameters.

### Usage

To create a new `Query` object, you can provide a field, an operation, and parameters:

```python
from did.query import Query

# Find all documents where the 'name' field is 'myname'
q1 = Query('base.name', 'exact_string', 'myname')

# Find all documents where the 'value' field is greater than 10
q2 = Query('my_data.value', 'greaterthan', 10)
```

You can also combine queries using the `&` (and) and `|` (or) operators:

```python
# Find all documents where the name is 'myname' AND the value is greater than 10
q3 = q1 & q2

# Find all documents where the name is 'myname' OR the value is greater than 10
q4 = q1 | q2
```

### Search Operations

The `Query` class supports a variety of search operations, including:

*   `exact_string`: Exact string match.
*   `regexp`: Regular expression match.
*   `contains_string`: Substring match.
*   `exact_number`: Exact number match.
*   `lessthan`, `greaterthan`, `lessthaneq`, `greaterthaneq`: Numeric comparisons.
*   `hasfield`: Checks for the existence of a field.
*   `isa`: Checks if a document is of a certain class or superclass.
*   `depends_on`: Checks for a specific dependency.