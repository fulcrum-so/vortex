use std::fmt;
use std::fmt::{Display, Write};

use humansize::{format_size, DECIMAL};

use crate::array::{Array, ArrayRef};

pub trait ArrayDisplay {
    fn fmt(&self, fmt: &'_ mut ArrayFormatter) -> fmt::Result;
}

pub struct ArrayFormatterWrapper<'a>(&'a dyn Array);

impl<'a> ArrayFormatterWrapper<'a> {
    pub fn new(array: &'a dyn Array) -> ArrayFormatterWrapper<'a> {
        ArrayFormatterWrapper(array)
    }
}

impl<'a, 'b: 'a> Display for ArrayFormatterWrapper<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let array = self.0;
        let mut array_fmt = ArrayFormatter::new(fmt, "".to_string(), array.nbytes());
        array_fmt.child("root", array)
    }
}

pub fn display_tree(array: &dyn Array) -> String {
    let mut string = String::new();
    write!(string, "{}", ArrayFormatterWrapper(array)).unwrap();
    string
}

pub struct ArrayFormatter<'a, 'b: 'a> {
    fmt: &'a mut fmt::Formatter<'b>,
    indent: String,
    total_size: usize,
}

impl<'a, 'b: 'a> ArrayFormatter<'a, 'b> {
    fn new(
        fmt: &'a mut fmt::Formatter<'b>,
        indent: String,
        total_size: usize,
    ) -> ArrayFormatter<'a, 'b> {
        ArrayFormatter {
            fmt,
            indent,
            total_size,
        }
    }

    pub fn property<T: Display>(&mut self, name: &str, value: T) -> fmt::Result {
        writeln!(self.fmt, "{}{}: {}", self.indent, name, value)
    }

    pub fn child(&mut self, name: &str, array: &dyn Array) -> fmt::Result {
        writeln!(
            self.fmt,
            "{}{}: {} nbytes={} ({:.2}%)",
            self.indent,
            name,
            array,
            format_size(array.nbytes(), DECIMAL),
            100f64 * array.nbytes() as f64 / self.total_size as f64
        )?;
        self.indent(|indent| ArrayDisplay::fmt(array, indent))
    }

    pub fn maybe_child(&mut self, name: &str, array: Option<&ArrayRef>) -> fmt::Result {
        if let Some(array) = array {
            self.child(&format!("{}?", name), array)
        } else {
            writeln!(self.fmt, "{}{}: None", self.indent, name)
        }
    }

    fn indent<F>(&mut self, indented: F) -> fmt::Result
    where
        F: FnOnce(&mut ArrayFormatter) -> fmt::Result,
    {
        let original_ident = self.indent.clone();
        self.indent += "  ";
        let res = indented(self);
        self.indent = original_ident;
        res
    }

    pub fn new_total_size<F>(&mut self, total: usize, new_total: F) -> fmt::Result
    where
        F: FnOnce(&mut ArrayFormatter) -> fmt::Result,
    {
        let original_total = self.total_size;
        self.total_size = total;
        let res = new_total(self);
        self.total_size = original_total;
        res
    }
}

#[cfg(test)]
mod test {
    use crate::array::ArrayRef;
    use crate::array::IntoArray;
    use crate::formatter::display_tree;

    #[test]
    fn display_primitive() {
        let arr: ArrayRef = (0..100).collect::<Vec<i32>>().into_array();
        assert_eq!(
            format!("{}", arr),
            "vortex.primitive(signed_int(32), len=100)"
        );
    }

    #[test]
    fn tree_display_primitive() {
        let arr: ArrayRef = (0..100).collect::<Vec<i32>>().into_array();
        assert_eq!(display_tree(&arr), "root: vortex.primitive(signed_int(32), len=100) nbytes=400 B (100.00%)\n  values: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]...\n  validity: None\n")
    }
}
