<h1>SQL Type Rules & Least Common Type</j1>

**NOTE**: “Type” here means “data type”.

---

**Contents**:

- [Type Wideness and Narrowness](#type-wideness-and-narrowness)
- [SQL Data Type Rules {#sql-data-type-rules}](#sql-data-type-rules-sql-data-type-rules)
  - [Promotion {#promotion}](#promotion-promotion)
  - [Implicit Downcasting {#implicit-downcasting}](#implicit-downcasting-implicit-downcasting)
  - [Implicit Crosscasting {#implicit-crosscasting}](#implicit-crosscasting-implicit-crosscasting)
  - [Explicit Type Casting {#explicit-type-casting}](#explicit-type-casting-explicit-type-casting)
- [Type Precedence List {#type-precedence-list}](#type-precedence-list-type-precedence-list)
- [Type Precedence Graph {#type-precedence-graph}](#type-precedence-graph-type-precedence-graph)
- [Least Common Type {#least-common-type}](#least-common-type-least-common-type)
- [Least Common Type Resolution](#least-common-type-resolution)
  - [Definition](#definition)
  - [Uses](#uses)
  - [Special Rules for Float \& String](#special-rules-for-float--string)

---

# Type Wideness and Narrowness

Given a data type D, a wider data type is one whose scope of application contains D but extends beyond D (i.e. this wider data type is more generalised as it is more widely applicable). A narrower type is one whose scope of application is contained in D but constrains its own scope of application more than D (i.e. this narrower data type is more specialised as it is applicable in a narrower range of contexts). “Application” here is defined without considering computation or memory constraints, i.e. it refers to a conceptual/algorithmic/theoretical-level application.

For example: INT is wider than TINYINT, whereas INT is narrower than FLOAT.

# SQL Data Type Rules {#sql-data-type-rules}

Databricks uses several rules to resolve conflicts among data types.

## Promotion {#promotion}

Safely expands a type to a wider type.

## Implicit Downcasting {#implicit-downcasting}

**N**arrows a type. This is the opposite of promotion.

## Implicit Crosscasting {#implicit-crosscasting}

Transforms a type into a type of another type family.

## Explicit Type Casting {#explicit-type-casting}

You can also explicitly cast between many types:

* cast function casts between most types, and returns errors if it cannot  
* try\_cast function is like cast but returns NULL when passed invalid values  
* Other builtin functions cast between types using provided format directives

# Type Precedence List {#type-precedence-list}

This list defines if values of a given type can be implicitly promoted to another type.

| Data type | Precedence list (from narrowest to widest) |
| :---- | :---- |
| TINYINT | TINYINT \-\> SMALLINT \-\> INT \-\> BIGINT \-\> DECIMAL \-\> FLOAT (1) \-\> DOUBLE |
| SMALLINT | SMALLINT \-\> INT \-\> BIGINT \-\> DECIMAL \-\> FLOAT (1) \-\> DOUBLE |
| INT | INT \-\> BIGINT \-\> DECIMAL \-\> FLOAT (1) \-\> DOUBLE |
| BIGINT | BIGINT \-\> DECIMAL \-\> FLOAT (1) \-\> DOUBLE |
| DECIMAL | DECIMAL \-\> FLOAT (1) \-\> DOUBLE |
| FLOAT | FLOAT (1) \-\> DOUBLE |
| DOUBLE | DOUBLE |
| DATE | DATE \-\> TIMESTAMP |
| TIMESTAMP | TIMESTAMP |
| ARRAY | ARRAY (2) |
| BINARY | BINARY |
| BOOLEAN | BOOLEAN |
| INTERVAL | INTERVAL |
| GEOGRAPHY | GEOGRAPHY(ANY) |
| GEOMETRY | GEOMETRY(ANY) |
| MAP | MAP (2) |
| STRING | STRING |
| STRUCT | STRUCT (2) |
| VARIANT | VARIANT |
| OBJECT | OBJECT (3) |

(1) For least common type resolution FLOAT is skipped to avoid loss of precision.  
(2) For complex types the precedence rule applies recursively to component elements.  
(3) OBJECT exists only within a VARIANT.  

> **Source**: [SQL data type rules | Databricks on Google Cloud](https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-datatype-rules#type-precedence-list)

# Type Precedence Graph {#type-precedence-graph}

![](./sql-type-rules-and-least-common-type.md)  

> **Source**: [SQL data type rules | Databricks on Google Cloud](https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-datatype-rules#type-precedence-graph)

Lower levels ⇒ Narrower types  
Higher levels ⇒ Wider types

# Least Common Type {#least-common-type}

Given a set of types T \= {type1, type2 …}, the least common type from T is the narrowest type reachable from the type precedence graph by all types of T. In other words, the least common type is the narrowest type that is an ancestor of each type in T. In effect, this means the least common type is the direct parent of at least one type in T.

# Least Common Type Resolution

> **Main reference**: [SQL data type rules | Databricks on Google Cloud](https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-datatype-rules#least-common-type-resolution)

## Definition

The process of obtaining the least common type among a set of types.

## Uses

The least common type resolution is used to:

- Narrow down the types of function parameters  
- Derive shared argument type for multiple parameters <br> *Useful for functions such as in, least, or greatest*  
- Derive the common type that subsumes the operand types for operators <br> *Such as arithmetic operations or comparisons*  
- Derive the result type for expressions such as the case expression  
- Derive the element, key, or value types for array and map constructors  
- Derive the result type of UNION, INTERSECT, or EXCEPT set operators

## Special Rules for Float & String

Special rules are applied if the least common type resolves to FLOAT. If any of the contributing types is an exact numeric type (TINYINT, SMALLINT, INTEGER, BIGINT, or DECIMAL) the least common type is pushed to DOUBLE to avoid potential loss of digits.

When the least common type is a STRING the collation is computed following the collation precedence rules (collation is a set of rules that determines how string comparisons are performed; learn more here: [Collation | Databricks on Google Cloud](https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-collation)).
