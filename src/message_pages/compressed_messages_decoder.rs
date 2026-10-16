use flate2::{Decompress, FlushDecompress, Status};
use my_service_bus::shared::protobuf_models::MessageProtobufModel;

/// How much is inflated in one go. Inflating a whole chunk at once would let a highly compressed
/// one blow up into the whole sub page; a step keeps the buffer at about one message.
const INFLATE_STEP: usize = 64 * 1024;

/// `MessagesProtobufModel.messages` is field 1. Anything else in the container is skipped.
const MESSAGES_FIELD_NO: u64 = 1;

const WIRE_TYPE_VARINT: u64 = 0;
const WIRE_TYPE_FIXED64: u64 = 1;
const WIRE_TYPE_LENGTH_DELIMITED: u64 = 2;
const WIRE_TYPE_FIXED32: u64 = 5;

/// A varint is at most 10 bytes.
const MAX_VARINT_LEN: usize = 10;

#[derive(Debug)]
#[allow(dead_code)]
pub enum CompressedMessagesError {
    /// The bytes are not a valid zlib stream, or its checksum does not match.
    Inflate(String),
    /// The zlib stream ended in the middle of a record.
    TruncatedRecord,
    /// There are bytes after the end of the zlib stream.
    TrailingData,
    Protobuf(String),
}

pub enum DecodedItem {
    Message(MessageProtobufModel),
    /// Give it the next piece of the compressed payload.
    NeedMoreInput,
    /// The zlib stream ended, its checksum matched, and every record was handed out.
    Finished,
}

/// Reads a sub page as it was archived - `zlib(MessagesProtobufModel)` - one message at a time,
/// with the compressed payload fed in whatever pieces it arrives in.
///
/// Neither the compressed nor the inflated sub page is ever held whole: what is buffered is the
/// piece that came last and the inflated bytes of the record being assembled.
///
/// The zlib checksum is only known at the end, so the messages handed out before it are the ones
/// a corrupted payload may have garbled. A garbled record almost always fails the protobuf decode
/// on its own; a stream that never reaches its end - or whose checksum fails - is an error rather
/// than a quiet end.
pub struct CompressedMessagesDecoder {
    decompress: Decompress,
    compressed: Vec<u8>,
    compressed_pos: usize,
    inflated: Vec<u8>,
    inflated_pos: usize,
    stream_ended: bool,
}

impl CompressedMessagesDecoder {
    pub fn new() -> Self {
        Self {
            // `true` - with the zlib header and the Adler-32 trailer, as `compress_payload` writes
            decompress: Decompress::new(true),
            compressed: Vec::new(),
            compressed_pos: 0,
            inflated: Vec::new(),
            inflated_pos: 0,
            stream_ended: false,
        }
    }

    pub fn add_compressed(&mut self, chunk: &[u8]) {
        if self.compressed_pos > 0 {
            self.compressed.drain(..self.compressed_pos);
            self.compressed_pos = 0;
        }

        self.compressed.extend_from_slice(chunk);
    }

    pub fn next(&mut self) -> Result<DecodedItem, CompressedMessagesError> {
        loop {
            if let Some(message) = self.take_message()? {
                return Ok(DecodedItem::Message(message));
            }

            if self.stream_ended {
                if self.inflated_pos < self.inflated.len() {
                    return Err(CompressedMessagesError::TruncatedRecord);
                }

                if self.compressed_pos < self.compressed.len() {
                    return Err(CompressedMessagesError::TrailingData);
                }

                return Ok(DecodedItem::Finished);
            }

            if !self.inflate_step()? {
                return Ok(DecodedItem::NeedMoreInput);
            }
        }
    }

    /// `false` when nothing could move without more input.
    fn inflate_step(&mut self) -> Result<bool, CompressedMessagesError> {
        if self.inflated_pos > 0 {
            self.inflated.drain(..self.inflated_pos);
            self.inflated_pos = 0;
        }

        // `decompress_vec` only ever writes into the spare capacity.
        self.inflated.reserve(INFLATE_STEP);

        let total_in = self.decompress.total_in();
        let total_out = self.decompress.total_out();

        let status = self
            .decompress
            .decompress_vec(
                &self.compressed[self.compressed_pos..],
                &mut self.inflated,
                FlushDecompress::None,
            )
            .map_err(|err| CompressedMessagesError::Inflate(format!("{}", err)))?;

        let consumed = (self.decompress.total_in() - total_in) as usize;
        let produced = self.decompress.total_out() - total_out;

        self.compressed_pos += consumed;

        match status {
            Status::StreamEnd => {
                self.stream_ended = true;
                Ok(true)
            }
            Status::Ok | Status::BufError => Ok(consumed > 0 || produced > 0),
        }
    }

    /// The next whole `messages` record out of what is inflated so far, skipping any other field.
    fn take_message(&mut self) -> Result<Option<MessageProtobufModel>, CompressedMessagesError> {
        loop {
            let buffer = &self.inflated[self.inflated_pos..];

            let Some(key) = read_varint(buffer)? else {
                return Ok(None);
            };

            let field_no = key.value >> 3;
            let wire_type = key.value & 0x07;

            let rest = &buffer[key.len..];

            let field_len = match wire_type {
                WIRE_TYPE_VARINT => match read_varint(rest)? {
                    Some(value) => value.len,
                    None => return Ok(None),
                },
                WIRE_TYPE_FIXED64 => 8,
                WIRE_TYPE_FIXED32 => 4,
                WIRE_TYPE_LENGTH_DELIMITED => {
                    let Some(len) = read_varint(rest)? else {
                        return Ok(None);
                    };

                    let total = key.len + len.len + len.value as usize;

                    if buffer.len() < total {
                        return Ok(None);
                    }

                    let body = &buffer[key.len + len.len..total];

                    if field_no == MESSAGES_FIELD_NO {
                        let message: MessageProtobufModel = prost::Message::decode(body)
                            .map_err(|err| CompressedMessagesError::Protobuf(format!("{}", err)))?;

                        self.inflated_pos += total;
                        return Ok(Some(message));
                    }

                    self.inflated_pos += total;
                    continue;
                }
                other => {
                    return Err(CompressedMessagesError::Protobuf(format!(
                        "unsupported wire type {} of field {}",
                        other, field_no
                    )))
                }
            };

            if rest.len() < field_len {
                return Ok(None);
            }

            self.inflated_pos += key.len + field_len;
        }
    }
}

struct Varint {
    value: u64,
    len: usize,
}

/// `Ok(None)` when the buffer ends inside the varint - wait for more.
fn read_varint(buffer: &[u8]) -> Result<Option<Varint>, CompressedMessagesError> {
    let mut value: u64 = 0;

    for (index, byte) in buffer.iter().enumerate() {
        if index == MAX_VARINT_LEN {
            return Err(CompressedMessagesError::Protobuf(
                "a varint longer than 10 bytes".to_string(),
            ));
        }

        value |= ((byte & 0x7F) as u64) << (7 * index);

        if byte & 0x80 == 0 {
            return Ok(Some(Varint {
                value,
                len: index + 1,
            }));
        }
    }

    if buffer.len() >= MAX_VARINT_LEN {
        return Err(CompressedMessagesError::Protobuf(
            "a varint longer than 10 bytes".to_string(),
        ));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use my_service_bus::abstractions::MessageId;
    use my_service_bus::shared::page_compressor::CompressedPageBuilder;
    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use super::*;

    fn message(id: i64, size: usize) -> MessageProtobufModel {
        MessageProtobufModel::new(
            MessageId::new(id),
            DateTimeAsMicroseconds::new(1_700_000_000_000_000 + id),
            (0..size).map(|itm| (itm * 7 + id as usize) as u8).collect(),
            vec![],
        )
    }

    fn compress(messages: &[MessageProtobufModel]) -> Vec<u8> {
        let mut builder = CompressedPageBuilder::new();

        for message in messages {
            builder.add_message(message);
        }

        builder.get_payload().unwrap()
    }

    /// Feeds `payload` in pieces of `piece` bytes, the way a socket hands them over.
    fn decode(
        payload: &[u8],
        piece: usize,
    ) -> Result<Vec<MessageProtobufModel>, CompressedMessagesError> {
        let mut decoder = CompressedMessagesDecoder::new();
        let mut pieces = payload.chunks(piece);
        let mut result = Vec::new();

        loop {
            match decoder.next()? {
                DecodedItem::Message(message) => result.push(message),
                DecodedItem::Finished => return Ok(result),
                DecodedItem::NeedMoreInput => match pieces.next() {
                    Some(piece) => decoder.add_compressed(piece),
                    None => return Err(CompressedMessagesError::TruncatedRecord),
                },
            }
        }
    }

    fn assert_same(expected: &[MessageProtobufModel], actual: &[MessageProtobufModel]) {
        assert_eq!(expected.len(), actual.len());

        for (expected, actual) in expected.iter().zip(actual) {
            assert_eq!(expected.get_message_id(), actual.get_message_id());
            assert_eq!(
                expected.get_created().unix_microseconds,
                actual.get_created().unix_microseconds
            );
            assert_eq!(expected.data, actual.data);
        }
    }

    /// Whatever the pieces - a byte, an odd size, the whole thing - the same messages come out.
    #[test]
    fn any_piece_size_gives_the_same_messages() {
        let messages: Vec<MessageProtobufModel> = (0..300).map(|id| message(id, 500)).collect();
        let payload = compress(messages.as_slice());

        for piece in [1, 7, 4096, payload.len()] {
            assert_same(
                messages.as_slice(),
                decode(payload.as_slice(), piece).unwrap().as_slice(),
            );
        }
    }

    /// A message far bigger than one inflate step is assembled across several.
    #[test]
    fn a_message_bigger_than_a_step_is_assembled() {
        let messages = vec![
            message(1, 10),
            message(2, INFLATE_STEP * 5 + 3),
            message(3, 10),
        ];
        let payload = compress(messages.as_slice());

        assert_same(
            messages.as_slice(),
            decode(payload.as_slice(), 1000).unwrap().as_slice(),
        );
    }

    #[test]
    fn an_empty_sub_page_finishes_at_once() {
        let payload = compress(&[]);

        assert!(decode(payload.as_slice(), 3).unwrap().is_empty());
    }

    /// Ending short must never look like the end of the sub page.
    #[test]
    fn a_truncated_payload_is_an_error() {
        let messages: Vec<MessageProtobufModel> = (0..50).map(|id| message(id, 200)).collect();
        let payload = compress(messages.as_slice());

        assert!(decode(&payload[..payload.len() / 2], 100).is_err());
        // Only the Adler-32 trailer missing
        assert!(decode(&payload[..payload.len() - 4], 100).is_err());
    }

    #[test]
    fn a_corrupted_payload_is_an_error() {
        let messages: Vec<MessageProtobufModel> = (0..50).map(|id| message(id, 200)).collect();
        let mut payload = compress(messages.as_slice());

        let last = payload.len() - 6;
        payload[last] ^= 0xFF;

        assert!(decode(payload.as_slice(), 100).is_err());
        assert!(decode(&[0u8, 1, 2, 3], 2).is_err());
    }

    #[test]
    fn bytes_after_the_stream_are_an_error() {
        let mut payload = compress(&[message(1, 10)]);
        payload.extend_from_slice(&[1, 2, 3]);

        assert!(matches!(
            decode(payload.as_slice(), 5),
            Err(CompressedMessagesError::TrailingData)
        ));
    }
}
