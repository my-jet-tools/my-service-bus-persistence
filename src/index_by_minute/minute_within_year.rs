#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Debug, Hash)]
pub struct MinuteWithinYear(u32);

impl MinuteWithinYear {
    pub fn new(minute: u32) -> Self {
        Self(minute)
    }

    pub fn get_position_in_file(&self) -> usize {
        self.0 as usize * 8
    }

    pub fn get_value(&self) -> u32 {
        self.0
    }

    pub fn as_ref(&self) -> &u32 {
        &self.0
    }
}

impl Into<MinuteWithinYear> for u32 {
    fn into(self) -> MinuteWithinYear {
        MinuteWithinYear::new(self)
    }
}
