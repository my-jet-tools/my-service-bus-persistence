use rust_extensions::BinaryPayloadBuilder;

use super::consts::TOC_STRUCTURE_SIZE;

/// Where a sub page sits inside its archive file. `length == 0` means the slot was never
/// written - a gap in message ids, or simply a sub page that has not been archived yet.
pub struct SubPagePosition {
    pub offset: u64,
    pub length: u32,
}

impl SubPagePosition {
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn parse(payload: &[u8]) -> Self {
        let mut offset = [0u8; 8];
        let mut length = [0u8; 4];

        offset.clone_from_slice(payload[0..8].as_ref());
        length.clone_from_slice(payload[8..12].as_ref());

        Self {
            offset: u64::from_le_bytes(offset),
            length: u32::from_le_bytes(length),
        }
    }

    pub fn serialize(&self) -> [u8; TOC_STRUCTURE_SIZE] {
        let mut payload = [0u8; TOC_STRUCTURE_SIZE];
        let mut buffer_builder = BinaryPayloadBuilder::new_as_slice(&mut payload);

        buffer_builder.write_u64(self.offset);
        buffer_builder.write_u32(self.length);

        payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let src = SubPagePosition {
            offset: 123_456,
            length: 789,
        };

        let dest = SubPagePosition::parse(src.serialize().as_slice());

        assert_eq!(src.offset, dest.offset);
        assert_eq!(src.length, dest.length);
        assert!(!dest.is_empty());
    }

    #[test]
    fn an_untouched_slot_is_empty() {
        let payload = [0u8; TOC_STRUCTURE_SIZE];
        assert!(SubPagePosition::parse(payload.as_slice()).is_empty());
    }
}
