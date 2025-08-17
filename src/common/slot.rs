#[derive(Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Debug)]
pub enum Slot {
    Init(usize),
    UnInit,
}

impl Slot {
    pub fn new(slot: usize) -> Self {
        Slot::Init(slot)
    }

    pub fn uninit() -> Self {
        Slot::UnInit
    }

    pub fn is_uninit(&self) -> bool {
        matches!(self, Slot::UnInit)
    }

    pub fn is_init(&self) -> bool {
        matches!(self, Slot::Init(_))
    }

    /// Panics: Panics if the slot was not initialised
    pub fn inner(&self) -> usize {
        assert!(self.is_init(), "Attempted to access uninitialized slot");

        match self {
            Slot::Init(slot) => *slot,
            Self::UnInit => {
                unreachable!("Tried to access an uninitialized slot")
            }
        }
    }
}

impl std::ops::Add<usize> for Slot {
    type Output = Slot;
    fn add(self, other: usize) -> Self::Output {
        match self {
            Slot::Init(slot) => Slot::Init(slot + other),

            // Since slot starts at 0, we can substract one from the adding value to accurately
            // show the value
            Slot::UnInit => {
                if other != 0 {
                    Slot::Init(other - 1)
                } else {
                    Slot::UnInit
                }
            }
        }
    }
}

impl std::ops::AddAssign<usize> for Slot {
    fn add_assign(&mut self, other: usize) {
        *self = match self {
            Slot::Init(slot) => Slot::Init(*slot + other),

            // Since slot starts at 0, we can substract one from the adding value to accurately
            // show the value
            Slot::UnInit => {
                if other != 0 {
                    Slot::Init(other - 1)
                } else {
                    Slot::UnInit
                }
            }
        };
    }
}

impl std::ops::Mul<usize> for Slot {
    type Output = Slot;
    fn mul(self, other: usize) -> Self::Output {
        match self {
            Slot::Init(slot) => Slot::Init(slot * other),
            Slot::UnInit => Slot::UnInit,
        }
    }
}

impl From<usize> for Slot {
    fn from(slot: usize) -> Self {
        Slot::Init(slot)
    }
}
