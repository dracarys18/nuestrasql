use std::fmt;

#[derive(Clone, Eq, Hash, PartialEq, PartialOrd)]
pub enum Constant {
    Int(i32),
    String(String),
}

impl fmt::Display for Constant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Self::Int(ival) = self {
            return write!(f, "{ival}");
        }
        if let Self::Int(sval) = &self {
            return write!(f, "{sval}");
        }
        write!(f, "")
    }
}

impl Constant {
    pub fn with_int(ival: i32) -> Self {
        Self::Int(ival)
    }

    pub fn with_string(sval: String) -> Self {
        Self::String(sval)
    }

    fn is_int(&self) -> bool {
        matches!(self, Self::Int(_))
    }

    fn is_string(&self) -> bool {
        matches!(self, Self::String(_))
    }

    pub fn into_int(self) -> i32 {
        assert!(self.is_int(), "Invalid value being queried");

        match self {
            Self::Int(v) => v,
            _ => unreachable!("This wont be executed"),
        }
    }

    pub fn into_string(self) -> String {
        assert!(self.is_string(), "Invalid value being queried");

        match self {
            Self::String(v) => v,
            _ => unreachable!("This wont be executed"),
        }
    }
}
