use pyo3::prelude::*;
use pyo3::intern;
use pyo3::sync::GILOnceCell;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use smallvec::SmallVec;
use std::sync::Arc;
use std::sync::OnceLock;

// Re-export serde_json::Value for JSON support
pub use serde_json::Value as JsonValue;

/// Cached reference to `object.__new__` for fast model instantiation.
/// This avoids the cost of `py.eval()` on every `to_models()` call.
static OBJECT_NEW: GILOnceCell<PyObject> = GILOnceCell::new();

/// Cached column names as a Python tuple (per QueryResult).
/// Initialized lazily on first access, avoiding repeated Vec cloning.
struct CachedColumnsTuple {
    /// The cached Python tuple of column names
    tuple: OnceLock<PyObject>,
}

/// Get or initialize the cached `object.__new__` reference.
#[inline]
fn get_object_new(py: Python<'_>) -> &PyObject {
    OBJECT_NEW.get_or_init(py, || {
        py.import("builtins")
            .expect("Failed to import builtins")
            .getattr("object")
            .expect("Failed to get object")
            .getattr("__new__")
            .expect("Failed to get __new__")
            .into()
    })
}

/// Intermediate row data that can be lazily converted to Python
#[derive(Clone, Debug)]
pub enum RowValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    /// JSON value - converted to Python dict/list via pythonize
    Json(JsonValue),
}

/// A row stored as column values (lazy conversion to Python)
/// Uses SmallVec to inline storage for rows with ≤16 columns (most tables),
/// avoiding heap allocation for typical workloads.
#[derive(Clone, Debug)]
pub struct LazyRow {
    pub values: SmallVec<[RowValue; 16]>,
}

/// Shared row data - wrapped in Arc to avoid cloning on iteration
pub type SharedRows = Arc<Vec<LazyRow>>;

/// Result from executing a SQL query
/// Uses lazy conversion - rows are stored as Rust data and converted to Python on demand
#[pyclass]
pub struct QueryResult {
    /// Rows stored as Rust data (Arc-wrapped to avoid cloning on iteration)
    rows: SharedRows,
    /// Column names (also Arc-wrapped)
    columns: Arc<Vec<String>>,
    /// Cached Python tuple of column names (lazy, avoids repeated Vec cloning)
    columns_tuple_cache: CachedColumnsTuple,
}

impl QueryResult {
    /// Create from lazy rows (optimized path)
    #[inline]
    pub fn from_lazy(rows: Vec<LazyRow>, columns: Vec<String>) -> Self {
        Self {
            rows: Arc::new(rows),
            columns: Arc::new(columns),
            columns_tuple_cache: CachedColumnsTuple {
                tuple: OnceLock::new(),
            },
        }
    }

    /// Get a reference to the rows
    #[inline]
    pub fn rows(&self) -> &[LazyRow] {
        &self.rows
    }

    /// Get or create a cached Python tuple of column names.
    /// This avoids repeated Vec cloning when accessing columns multiple times.
    #[inline]
    fn get_columns_tuple<'py>(&self, py: Python<'py>) -> &PyObject {
        self.columns_tuple_cache.tuple.get_or_init(|| {
            let tuple = PyTuple::new(py, self.columns.iter().map(|s| s.as_str()))
                .expect("Failed to create columns tuple");
            tuple.into()
        })
    }
}

/// Convert RowValue to Python object - hyper-optimized version
#[inline(always)]
fn row_value_to_py(py: Python<'_>, val: &RowValue) -> PyObject {
    match val {
        RowValue::Null => py.None(),
        RowValue::Bool(b) => b.to_object(py),
        RowValue::Int(i) => i.to_object(py),
        RowValue::Float(f) => f.to_object(py),
        RowValue::String(s) => s.to_object(py),
        RowValue::Bytes(b) => b.to_object(py),
        RowValue::Json(json) => {
            // Use pythonize to convert serde_json::Value to Python dict/list
            // This is very fast as pythonize is optimized for this conversion
            pythonize::pythonize(py, json)
                .map(|bound| bound.unbind())
                .unwrap_or_else(|_| py.None())
        }
    }
}

/// Convert a single row to a Python dict
#[inline]
fn row_to_dict<'py>(
    py: Python<'py>,
    row: &LazyRow,
    cols: &[String],
    interned_cols: Option<&[Bound<'py, PyString>]>,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    let vals = &row.values;
    let len = cols.len().min(vals.len());

    match interned_cols {
        Some(interned) => {
            for i in 0..len {
                let py_val = row_value_to_py(py, unsafe { vals.get_unchecked(i) });
                dict.set_item(unsafe { interned.get_unchecked(i) }, py_val)?;
            }
        }
        None => {
            for i in 0..len {
                let py_val = row_value_to_py(py, unsafe { vals.get_unchecked(i) });
                dict.set_item(unsafe { cols.get_unchecked(i) }, py_val)?;
            }
        }
    }
    Ok(dict)
}

#[pymethods]
impl QueryResult {
    /// Get all rows as a list of dictionaries - optimized
    fn all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let rows = &self.rows;
        let cols = self.columns.as_ref();

        if rows.is_empty() {
            return PyList::new(py, Vec::<PyObject>::new());
        }

        // Pre-intern column names for faster dict key setting
        let interned_cols: Vec<Bound<'py, PyString>> = cols.iter()
            .map(|c| PyString::intern(py, c))
            .collect();

        // Build all dicts
        let dicts: PyResult<Vec<Bound<'py, PyDict>>> = rows.iter()
            .map(|row| row_to_dict(py, row, cols, Some(&interned_cols)))
            .collect();

        PyList::new(py, dicts?)
    }

    /// Get the first row, or None if empty
    #[inline]
    fn first<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        if let Some(row) = self.rows.first() {
            Ok(Some(row_to_dict(py, row, &self.columns, None)?))
        } else {
            Ok(None)
        }
    }

    /// Get a single row, raising error if not exactly one row
    fn one<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        if self.rows.len() != 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Expected exactly 1 row, got {}",
                self.rows.len()
            )));
        }
        self.first(py)?.ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("Expected exactly 1 row, got 0")
        })
    }

    /// Get a single row or None
    fn one_or_none<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        match self.rows.len() {
            0 => Ok(None),
            1 => self.first(py),
            n => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Expected at most 1 row, got {}",
                n
            ))),
        }
    }

    /// Get column names as a tuple (cached, no allocation on repeated access)
    #[getter]
    fn columns<'py>(&self, py: Python<'py>) -> PyObject {
        self.get_columns_tuple(py).clone_ref(py)
    }

    /// Get column names as a list (for compatibility, allocates new list)
    fn column_names(&self) -> Vec<String> {
        self.columns.as_ref().clone()
    }

    /// Get the number of rows returned
    #[getter]
    fn rowcount(&self) -> usize {
        self.rows.len()
    }

    /// Check if result is empty
    #[inline]
    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    #[inline]
    fn __len__(&self) -> usize {
        self.rows.len()
    }

    /// Iterator that borrows from Arc instead of cloning
    fn __iter__(&self) -> QueryResultIter {
        QueryResultIter {
            rows: Arc::clone(&self.rows),
            columns: Arc::clone(&self.columns),
            index: 0,
        }
    }

    fn __repr__(&self) -> String {
        format!("<QueryResult rows={}>", self.rows.len())
    }

    /// Get rows as list of tuples (faster than dicts for large results)
    fn tuples<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let rows = &self.rows;

        let tuples: PyResult<Vec<Bound<'py, PyTuple>>> = rows.iter().map(|row| {
            let values: Vec<PyObject> = row.values.iter()
                .map(|v| row_value_to_py(py, v))
                .collect();
            PyTuple::new(py, values)
        }).collect();

        PyList::new(py, tuples?)
    }

    /// Get a specific column as a list (fast column extraction)
    fn column<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyList>> {
        let col_idx = self.columns.iter().position(|c| c == name)
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(format!("Column '{}' not found", name)))?;

        let values: Vec<PyObject> = self.rows.iter().map(|row| {
            row.values.get(col_idx)
                .map(|v| row_value_to_py(py, v))
                .unwrap_or_else(|| py.None())
        }).collect();

        PyList::new(py, values)
    }

    /// Get multiple columns as a list of tuples (efficient for projections)
    fn columns_as_tuples<'py>(&self, py: Python<'py>, names: Vec<String>) -> PyResult<Bound<'py, PyList>> {
        let indices: Vec<usize> = names.iter()
            .map(|name| {
                self.columns.iter().position(|c| c == name)
                    .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(format!("Column '{}' not found", name)))
            })
            .collect::<PyResult<Vec<_>>>()?;

        let tuples: PyResult<Vec<Bound<'py, PyTuple>>> = self.rows.iter().map(|row| {
            let values: Vec<PyObject> = indices.iter()
                .map(|&idx| {
                    row.values.get(idx)
                        .map(|v| row_value_to_py(py, v))
                        .unwrap_or_else(|| py.None())
                })
                .collect();
            PyTuple::new(py, values)
        }).collect();

        PyList::new(py, tuples?)
    }

    /// Get a scalar value from first row, first column
    #[inline]
    fn scalar<'py>(&self, py: Python<'py>) -> PyObject {
        self.rows.first()
            .and_then(|row| row.values.first())
            .map(|val| row_value_to_py(py, val))
            .unwrap_or_else(|| py.None())
    }

    /// Get all values from first column as Python objects
    fn scalars<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let values: Vec<PyObject> = self.rows.iter().map(|row| {
            row.values.first()
                .map(|val| row_value_to_py(py, val))
                .unwrap_or_else(|| py.None())
        }).collect();

        PyList::new(py, values)
    }

    /// Create model instances using Python's _from_row_fast for proper JSON handling.
    /// This delegates to Python for type conversions (JSON deserialization, etc.)
    fn to_models<'py>(&self, py: Python<'py>, model_class: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyList>> {
        let rows = &self.rows;
        let cols = self.columns.as_ref();

        if rows.is_empty() {
            return PyList::new(py, Vec::<PyObject>::new());
        }

        // Get Python's _from_row_fast method for proper type handling
        let from_row_fast = model_class.getattr(intern!(py, "_from_row_fast"))?;

        // Pre-intern column names for faster dict creation
        let interned_cols: Vec<Bound<'py, PyString>> = cols.iter()
            .map(|col| PyString::intern(py, col))
            .collect();

        // Pre-allocate the result vector
        let mut instances: Vec<PyObject> = Vec::with_capacity(rows.len());

        for row in rows.iter() {
            // Build dict from row values
            let dict = PyDict::new(py);
            let vals = &row.values;
            let len = cols.len().min(vals.len());

            for i in 0..len {
                let py_val = row_value_to_py(py, &vals[i]);
                dict.set_item(&interned_cols[i], py_val)?;
            }

            // Call _from_row_fast(dict) to create instance with proper type handling
            let instance = from_row_fast.call1((dict,))?;
            instances.push(instance.unbind());
        }

        PyList::new(py, instances)
    }

    /// Create a single model instance from the first row
    fn to_model<'py>(&self, py: Python<'py>, model_class: &Bound<'py, PyAny>) -> PyResult<Option<PyObject>> {
        if self.rows.is_empty() {
            return Ok(None);
        }

        let row = &self.rows[0];
        let cols = self.columns.as_ref();

        // Get Python's _from_row_fast method for proper type handling (JSON, etc.)
        let from_row_fast = model_class.getattr(intern!(py, "_from_row_fast"))?;

        // Build dict from row values
        let dict = PyDict::new(py);
        let vals = &row.values;
        let len = cols.len().min(vals.len());

        for i in 0..len {
            let col = &cols[i];
            let py_val = row_value_to_py(py, &vals[i]);
            dict.set_item(PyString::intern(py, col), py_val)?;
        }

        // Call _from_row_fast(dict) to create instance with proper type handling
        let instance = from_row_fast.call1((dict,))?;
        Ok(Some(instance.unbind()))
    }
}

/// Iterator over query results - uses Arc to avoid cloning row data
#[pyclass]
pub struct QueryResultIter {
    rows: SharedRows,
    columns: Arc<Vec<String>>,
    index: usize,
}

#[pymethods]
impl QueryResultIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        if self.index < self.rows.len() {
            let row = &self.rows[self.index];
            self.index += 1;

            let dict = row_to_dict(py, row, &self.columns, None)?;
            Ok(Some(dict.into()))
        } else {
            Ok(None)
        }
    }
}
