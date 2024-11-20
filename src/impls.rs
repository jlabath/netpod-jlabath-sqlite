use crate::{
    DescribeResponse, ErrorResponse, InvokeResponse, Namespace, Op, Request, Response, Status, Var,
};
use bendy::decoding::{Error as BdecodeError, FromBencode, Object, ResultExt};
use bendy::encoding::{AsString, Error as BencodeError, SingleItemEncoder, ToBencode};

impl FromBencode for Request {
    fn decode_bencode_object(object: Object) -> Result<Self, BdecodeError> {
        let mut op = Op::Describe;
        let mut id = None;
        let mut var = None;
        let mut args = None;
        let mut dict = object.try_into_dictionary()?;
        while let Some(pair) = dict.next_pair()? {
            match pair {
                (b"id", value) => {
                    id = String::decode_bencode_object(value)
                        .context("id")
                        .map(Some)?;
                }
                (b"var", value) => {
                    var = String::decode_bencode_object(value)
                        .context("var")
                        .map(Some)?;
                }
                (b"args", value) => {
                    args = String::decode_bencode_object(value)
                        .context("args")
                        .map(Some)?;
                }
                (b"op", value) => {
                    let op_str = String::decode_bencode_object(value).context("op")?;
                    match Op::from_str(&op_str) {
                        Ok(oop) => {
                            op = oop;
                        }
                        Err(s) => eprintln!("trouble decoding op: {}", &s),
                    }
                }
                (unknown_field, _) => {
                    return Err(BdecodeError::unexpected_field(String::from_utf8_lossy(
                        unknown_field,
                    )));
                }
            }
        }

        Ok(Request { args, id, op, var })
    }
}

impl ToBencode for Var {
    const MAX_DEPTH: usize = 1;

    fn encode(&self, encoder: SingleItemEncoder) -> Result<(), BencodeError> {
        encoder.emit_dict(|mut e| {
            e.emit_pair(b"name", &self.name)?;
            Ok(())
        })
    }
}

impl ToBencode for Namespace {
    const MAX_DEPTH: usize = 3;

    fn encode(&self, encoder: SingleItemEncoder) -> Result<(), BencodeError> {
        encoder.emit_dict(|mut e| {
            e.emit_pair(b"name", &self.name)?;
            e.emit_pair(b"vars", &self.vars)?;
            Ok(())
        })
    }
}

impl ToBencode for DescribeResponse {
    const MAX_DEPTH: usize = 5;

    fn encode(&self, encoder: SingleItemEncoder) -> Result<(), BencodeError> {
        encoder.emit_dict(|mut e| {
            e.emit_pair(b"format", &self.format)?;
            e.emit_pair(b"namespaces", &self.namespaces)?;
            Ok(())
        })
    }
}

impl ToBencode for Status {
    const MAX_DEPTH: usize = 0;

    fn encode(&self, encoder: SingleItemEncoder) -> Result<(), BencodeError> {
        encoder.emit_str(self.as_str())
    }
}

impl ToBencode for ErrorResponse {
    const MAX_DEPTH: usize = 1;

    fn encode(&self, encoder: SingleItemEncoder) -> Result<(), BencodeError> {
        encoder.emit_dict(|mut e| {
            e.emit_pair(b"ex-message", &self.ex_message)?;
            if let Some(rid) = &self.id {
                e.emit_pair(b"id", rid)?;
            }
            e.emit_pair(b"status", &self.status)?;
            Ok(())
        })
    }
}

impl ToBencode for InvokeResponse {
    const MAX_DEPTH: usize = 1;

    fn encode(&self, encoder: SingleItemEncoder) -> Result<(), BencodeError> {
        encoder.emit_dict(|mut e| {
            e.emit_pair(b"id", &self.id)?;
            e.emit_pair(b"status", &self.status)?;
            e.emit_pair(b"value", AsString(&self.value))?;
            Ok(())
        })
    }
}

impl ToBencode for Response {
    const MAX_DEPTH: usize = 6;

    fn encode(&self, enc: SingleItemEncoder) -> Result<(), BencodeError> {
        match self {
            Self::Error(r) => enc.emit(r),
            Self::Describe(r) => enc.emit(r),
            Self::Invoke(r) => enc.emit(r),
        }
    }
}
