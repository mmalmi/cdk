use std::fmt::Debug;

use cdk_sql_common::value::Value;
use tokio_postgres::types::{self, FromSql, ToSql};

#[derive(Debug)]
pub enum PgValue<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a str),
    Blob(&'a [u8]),
}

impl<'a> From<&'a Value> for PgValue<'a> {
    fn from(value: &'a Value) -> Self {
        match value {
            Value::Blob(b) => PgValue::Blob(b),
            Value::Text(text) => PgValue::Text(text.as_str()),
            Value::Null => PgValue::Null,
            Value::Integer(i) => PgValue::Integer(*i),
            Value::Real(r) => PgValue::Real(*r),
        }
    }
}

impl<'a> From<PgValue<'a>> for Value {
    fn from(val: PgValue<'a>) -> Self {
        match val {
            PgValue::Blob(value) => Value::Blob(value.to_owned()),
            PgValue::Text(value) => Value::Text(value.to_owned()),
            PgValue::Null => Value::Null,
            PgValue::Integer(n) => Value::Integer(n),
            PgValue::Real(r) => Value::Real(r),
        }
    }
}

impl<'a> FromSql<'a> for PgValue<'a> {
    fn accepts(_ty: &types::Type) -> bool {
        true
    }

    fn from_sql(
        ty: &types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(match *ty {
            types::Type::VARCHAR | types::Type::TEXT | types::Type::BPCHAR | types::Type::NAME => {
                PgValue::Text(<&str as FromSql>::from_sql(ty, raw)?)
            }
            types::Type::BOOL => PgValue::Integer(if <bool as FromSql>::from_sql(ty, raw)? {
                1
            } else {
                0
            }),
            types::Type::INT2 => PgValue::Integer(<i16 as FromSql>::from_sql(ty, raw)? as i64),
            types::Type::INT4 => PgValue::Integer(<i32 as FromSql>::from_sql(ty, raw)? as i64),
            types::Type::INT8 => PgValue::Integer(<i64 as FromSql>::from_sql(ty, raw)?),
            types::Type::BIT_ARRAY | types::Type::BYTEA | types::Type::UNKNOWN => {
                PgValue::Blob(<&[u8] as FromSql>::from_sql(ty, raw)?)
            }
            _ => panic!("Unsupported type {ty:?}"),
        })
    }

    fn from_sql_null(_ty: &types::Type) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(PgValue::Null)
    }
}

impl ToSql for PgValue<'_> {
    fn to_sql(
        &self,
        ty: &types::Type,
        out: &mut types::private::BytesMut,
    ) -> Result<types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match self {
            PgValue::Blob(blob) => (*blob).to_sql(ty, out),
            PgValue::Text(text) => (*text).to_sql(ty, out),
            PgValue::Null => Ok(types::IsNull::Yes),
            PgValue::Real(r) => r.to_sql(ty, out),
            PgValue::Integer(i) => match *ty {
                types::Type::BOOL => (*i != 0).to_sql(ty, out),
                types::Type::INT2 => i16::try_from(*i)?.to_sql(ty, out),
                types::Type::INT4 => i32::try_from(*i)?.to_sql(ty, out),
                _ => i.to_sql_checked(ty, out),
            },
        }
    }

    fn accepts(_ty: &types::Type) -> bool
    where
        Self: Sized,
    {
        true
    }

    fn encode_format(&self, ty: &types::Type) -> types::Format {
        match self {
            PgValue::Blob(blob) => blob.encode_format(ty),
            PgValue::Text(text) => text.encode_format(ty),
            PgValue::Null => types::Format::Text,
            PgValue::Real(r) => r.encode_format(ty),
            PgValue::Integer(i) => i.encode_format(ty),
        }
    }

    fn to_sql_checked(
        &self,
        ty: &types::Type,
        out: &mut types::private::BytesMut,
    ) -> Result<types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            PgValue::Blob(blob) => blob.to_sql_checked(ty, out),
            PgValue::Text(text) => text.to_sql_checked(ty, out),
            PgValue::Null => Ok(types::IsNull::Yes),
            PgValue::Real(r) => r.to_sql_checked(ty, out),
            PgValue::Integer(i) => match *ty {
                types::Type::BOOL => (*i != 0).to_sql_checked(ty, out),
                types::Type::INT2 => i16::try_from(*i)?.to_sql_checked(ty, out),
                types::Type::INT4 => i32::try_from(*i)?.to_sql_checked(ty, out),
                _ => i.to_sql_checked(ty, out),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int2_bounds_roundtrip_and_overflow_rejects() {
        check_bounds(types::Type::INT2, i64::from(i16::MIN), i64::from(i16::MAX));
    }

    #[test]
    fn int4_bounds_roundtrip_and_overflow_rejects() {
        check_bounds(types::Type::INT4, i64::from(i32::MIN), i64::from(i32::MAX));
    }

    fn check_bounds(ty: types::Type, min: i64, max: i64) {
        for checked in [false, true] {
            for value in [min, 0, max] {
                let mut out = types::private::BytesMut::new();
                let number = PgValue::Integer(value);
                if checked {
                    number.to_sql_checked(&ty, &mut out).unwrap();
                } else {
                    number.to_sql(&ty, &mut out).unwrap();
                }
                assert!(
                    matches!(PgValue::from_sql(&ty, &out).unwrap(), PgValue::Integer(actual) if actual == value)
                );
            }
            for value in [min - 1, max + 1] {
                let mut out = types::private::BytesMut::new();
                let number = PgValue::Integer(value);
                let result = if checked {
                    number.to_sql_checked(&ty, &mut out)
                } else {
                    number.to_sql(&ty, &mut out)
                };
                assert!(result.is_err());
                assert!(out.is_empty());
            }
        }
    }
}
