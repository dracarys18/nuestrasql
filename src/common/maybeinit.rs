// Wrapper enum to allow late initialization.
pub(crate) enum MaybeInit<T> {
    Init(T),
    NoInit,
}

impl<T> MaybeInit<T> {
    /// Creates a new uninitialized MaybeInit
    pub(crate) fn new() -> Self {
        Self::NoInit
    }

    /// Initializes or overwrites the value
    pub(crate) fn overwrite(&mut self, val: T) {
        *self = Self::Init(val);
    }

    /// Checks if the value is initialized
    pub(crate) fn is_init(&self) -> bool {
        matches!(self, Self::Init(_))
    }

    /// Take ownership of the value, leaving `NoInit`
    pub(crate) fn take(&mut self) -> Option<T> {
        match std::mem::replace(self, Self::NoInit) {
            Self::Init(v) => Some(v),
            Self::NoInit => None,
        }
    }
}

impl<T> std::ops::Deref for MaybeInit<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Init(val) => val,
            Self::NoInit => panic!("Attempted to deref MaybeInit before initialization"),
        }
    }
}

impl<T> std::ops::DerefMut for MaybeInit<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Init(val) => val,
            Self::NoInit => panic!("Attempted to deref MaybeInit before initialization"),
        }
    }
}
