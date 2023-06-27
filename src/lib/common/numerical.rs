use crate::schema::types::Type;
use evalexpr::Value;
use std::cmp::Ordering;

#[cfg(target_pointer_width = "32")]
const USIZE_LENGTH: usize = 4;

#[cfg(target_pointer_width = "64")]
const USIZE_LENGTH: usize = 8;

pub trait MultiFieldCmp: Ord {
    type Item;
    fn multi_cmp(&self, other: Self::Item, desc_vec: &[bool]) -> Ordering;
}

/// Helper trait on byte slices to reinterpret the bytes at an offset as distinct data types
pub trait ByteMagic {
    // TODO - Add Monetary types
    fn extract_usize(&self, offset: usize) -> usize;
    fn extract_u16(&self, offset: usize) -> u16;
    fn extract_u32(&self, offset: usize) -> u32;
    fn extract_u64(&self, offset: usize) -> u64;
    fn extract_f32(&self, offset: usize) -> f32;
    fn extract_f64(&self, offset: usize) -> f64;
    fn to_usize(self) -> usize;
    fn to_u16(self) -> u16;
    fn to_u32(self) -> u32;
    fn to_u64(self) -> u64;
    fn to_f32(self) -> f32;
    fn to_f64(self) -> f64;
    fn to_i16(self) -> i16;
    fn to_i32(self) -> i32;
    fn to_i64(self) -> i64;
}

impl ByteMagic for &[u8] {
    /// Extract a USIZE from a byte slice starting from offset
    ///
    /// The size of usize is platform dependant : 4 bytes on 32-bit systems and 8 bytes  on 64-bit systems
    fn extract_usize(&self, offset: usize) -> usize {
        let size = USIZE_LENGTH;
        let bytes = &self[offset..(offset + size)];
        usize::from_ne_bytes(bytes.try_into().unwrap())
    }

    /// Extact a U16 from byte slice starting from offset by reading 2 consecutive bytes
    fn extract_u16(&self, offset: usize) -> u16 {
        let bytes = [self[offset], self[offset + 1]];
        u16::from_ne_bytes(bytes)
    }

    /// Extact a U32 from byte slice starting from offset by reading 4 consecutive bytes
    fn extract_u32(&self, offset: usize) -> u32 {
        let bytes = &self[offset..(offset + 4)];
        u32::from_ne_bytes(bytes.try_into().unwrap())
    }
    /// Extact a U64 from byte slice starting from offset by reading 8 consecutive bytes
    fn extract_u64(&self, offset: usize) -> u64 {
        let bytes = &self[offset..(offset + 8)];
        u64::from_ne_bytes(bytes.try_into().unwrap())
    }

    /// Extact an F32 from byte slice starting from offset by reading 4 consecutive bytes
    fn extract_f32(&self, offset: usize) -> f32 {
        let bytes = &self[offset..(offset + 4)];
        f32::from_ne_bytes(bytes.try_into().unwrap())
    }

    /// Extact an F64 from byte slice starting from offset by reading 8 consecutive bytes
    fn extract_f64(&self, offset: usize) -> f64 {
        let bytes = &self[offset..(offset + 8)];
        f64::from_ne_bytes(bytes.try_into().unwrap())
    }

    fn to_usize(self) -> usize {
        usize::from_ne_bytes(self.try_into().unwrap())
    }

    fn to_u16(self) -> u16 {
        u16::from_ne_bytes(self.try_into().unwrap())
    }

    fn to_u32(self) -> u32 {
        u32::from_ne_bytes(self.try_into().unwrap())
    }

    fn to_u64(self) -> u64 {
        u64::from_ne_bytes(self.try_into().unwrap())
    }

    fn to_f32(self) -> f32 {
        f32::from_ne_bytes(self.try_into().unwrap())
    }

    fn to_f64(self) -> f64 {
        f64::from_ne_bytes(self.try_into().unwrap())
    }

    fn to_i16(self) -> i16 {
        i16::from_ne_bytes(self.try_into().unwrap())
    }

    fn to_i32(self) -> i32 {
        i32::from_ne_bytes(self.try_into().unwrap())
    }

    fn to_i64(self) -> i64 {
        i64::from_ne_bytes(self.try_into().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use crate::common::numerical::ByteMagic;

    #[test]
    fn test_extract_u16() {
        let v = [0_u8, 2, 1, 44, 1, 5, 6, 7];
        let result = v.as_slice().extract_u16(3);
        assert_eq!(result, 300_u16)
    }
}
