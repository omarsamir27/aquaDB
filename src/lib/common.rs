pub mod numerical{
    use std::intrinsics::size_of;

    pub trait ByteMagic {
        // TODO - Add Monetary types
        fn extract_usize(&self,offset:usize) -> usize;
        fn extract_u16(&self,offset:usize) -> u16;
        fn extract_u32(&self,offset:usize) -> u32;
        fn extract_u64(&self,offset:usize) -> u64;
        fn extract_f32(&self,offset:usize) -> f32;
        fn extract_f64(&self,offset:usize) -> f64;
    }

    impl ByteMagic for &[u8]{
        fn extract_usize(&self, offset: usize) -> usize {
            let size = size_of::<usize>() - 1 ;
            let bytes = &self[offset ..(offset + size)];
            usize::from_ne_bytes(bytes.try_into().unwrap())
        }


        fn extract_u16(&self,offset:usize) -> u16 {
            let bytes = [self[offset],self[offset + 1]];
            u16::from_ne_bytes(bytes)
        }

        fn extract_u32(&self, offset: usize) -> u32 {
            let bytes = &self[offset..(offset+3)];
            u32::from_ne_bytes(bytes.try_into().unwrap())
        }

        fn extract_u64(&self, offset: usize) -> u64 {
            let bytes = &self[offset..(offset+7)];
            u64::from_ne_bytes(bytes.try_into().unwrap())
        }

        fn extract_f32(&self, offset: usize) -> f32 {
            let bytes = &self[offset..(offset+3)];
            f32::from_ne_bytes(bytes.try_into().unwrap())
        }

        fn extract_f64(&self, offset: usize) -> f64 {
            let bytes = &self[offset..(offset+7)];
            f64::from_ne_bytes(bytes.try_into().unwrap())
        }
    }
}

#[cfg(test)]
mod tests{
    use crate::common::numerical::ByteMagic;

    #[test]
    fn test_extract_u16(){
        use super::numerical::ByteMagic;
        let v  = [0_u8,2,1,44,1,5,6,7];
        let result = v.as_slice().extract_u16(3);
        assert_eq!(result,300_u16)
    }
}