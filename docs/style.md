# Style

## Architecture

Individual data processing components should typically only do one thing: taking a single input stream, transforming it and producing an output stream.
Should a data format boundary need to be crossed (e.g. file loading or saving) this should be done by a dedicated utility.

## Code

In general whatever `treefmt` (and it's downstream formatters) dictates the code style to use.

In cases where the formatter does not care, the following rules apply:

### :crab: No empty lines in `use` statements

```rust
use crate::Something;
use super::SomethingElse
use digital_muon_common::Time
use std::time::Duration
use tokio::task::JoinHandle;
```

instead of

```rust
use crate::Something;

use tokio::task::JoinHandle;
use digital_muon_common::Time

use std::time::Duration
use super::SomethingElse
```

### :crab: One empty line between `fn`, `impl`, and `mod` block

```rust
fn one() -> i32 {
  1
}

fn two() -> i32 {
  1
}
```

instead of

```rust
fn one() -> i32 {
  1
}
fn two() -> i32 {
  1
}
```

### :crab: Error handling rules

TL;DR: follow the [`miette` using guide](https://docs.rs/miette/latest/miette/#using).

The following are prohibited:

- `unwrap()` (outside of automated tests)
- `panic!()`
- `anyhow`

The following are discouraged:

- `[]` (use `get()` with appropriate error handling instead)

Unchecked equivalents may be used for performance reasons when accompanied by
appropriate in-code documentation of why the operation is still safe.
