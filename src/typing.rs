#[derive(Clone, Copy)]
pub struct Year(u32);

impl Year {
    pub fn new(year: u32) -> Self {
        Year(year)
    }

    pub fn get_value(&self) -> u32 {
        self.0
    }

    pub fn value_as_ref(&self) -> &u32 {
        &self.0
    }
}

impl Into<u32> for Year {
    fn into(self) -> u32 {
        self.0
    }
}

impl Into<Year> for u32 {
    fn into(self) -> Year {
        Year(self)
    }
}
