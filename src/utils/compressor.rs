use snap::raw::{Decoder as SnapDecoder, Encoder as SnapEncoder};
use zstd::{decode_all, encode_all};

use crate::db::{CompressionType, DError, DResult};

pub trait Compressor {
    fn compress(&self, _input: &[u8]) -> DResult<Vec<u8>> {
        unimplemented!()
    }

    fn decompress(&self, _input: &[u8]) -> DResult<Vec<u8>> {
        unimplemented!()
    }
}
pub struct ErrorCompressor;

impl Compressor for ErrorCompressor {}

pub struct SnappyCompressor;

impl Compressor for SnappyCompressor {
    fn compress(&self, input: &[u8]) -> DResult<Vec<u8>> {
        let mut s = SnapEncoder::new();
        s.compress_vec(input)
            .map_err(|_| DError::CustomError("Snappy compress error"))
    }

    fn decompress(&self, input: &[u8]) -> DResult<Vec<u8>> {
        let mut s = SnapDecoder::new();
        s.decompress_vec(input)
            .map_err(|_| DError::CustomError("Snappy decompress error"))
    }
}

pub struct ZSTDCompressor;

impl Compressor for ZSTDCompressor {
    fn compress(&self, input: &[u8]) -> DResult<Vec<u8>> {
        encode_all(input, 3).map_err(|_| DError::CustomError("ZSTD compress error"))
    }

    fn decompress(&self, input: &[u8]) -> DResult<Vec<u8>> {
        decode_all(input).map_err(|_| DError::CustomError("ZSTD decompress error"))
    }
}

pub fn generate_compressor(t: CompressionType) -> Box<dyn Compressor> {
    match t {
        // simple constructing
        CompressionType::NoCompress => Box::new(ErrorCompressor),
        CompressionType::Snappy => Box::new(SnappyCompressor),
        CompressionType::Zstd => Box::new(ZSTDCompressor),
    }
}

#[cfg(test)]
mod test {
    use crate::db::CompressionType;
    use crate::utils::compressor::generate_compressor;

    #[test]
    #[should_panic]
    fn test_error() {
        let s = CompressionType::NoCompress;
        let f = generate_compressor(s);
        f.compress(&[]).unwrap();
    }

    #[test]
    fn test_compressor() {
        let data:&[u8]  = b"Some birds aren't meant to be caged, that's all. Their feathers are just too bright...";
        let s = CompressionType::Snappy;
        let f = generate_compressor(s);
        let ret = f.compress(&data).unwrap();
        assert_eq!(data, f.decompress(&ret).unwrap());

        let s = CompressionType::Zstd;
        let f = generate_compressor(s);
        let ret = f.compress(&data).unwrap();
        assert_eq!(data, f.decompress(&ret).unwrap());
    }
}
