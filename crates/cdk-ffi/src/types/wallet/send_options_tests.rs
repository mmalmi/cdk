use super::{decode_send_options, encode_send_options, Amount, Id, SendOptions};

#[test]
fn funding_constraints_survive_json_and_core_roundtrip() {
    let options = SendOptions {
        retain_cancellation: true,
        max_total_amount: Some(Amount::new(41)),
        required_keyset_id: Some(Id {
            hex: "009a1f293253e41e".to_string(),
        }),
        ..Default::default()
    };
    let json = encode_send_options(options).unwrap();
    let core: cdk::wallet::SendOptions = decode_send_options(json).unwrap().try_into().unwrap();
    assert!(core.retain_cancellation);
    assert_eq!(core.max_total_amount, Some(cdk::Amount::from(41)));
    assert_eq!(
        core.required_keyset_id.unwrap().to_string(),
        "009a1f293253e41e"
    );
    let ffi = SendOptions::from(core);
    assert!(ffi.retain_cancellation);
    assert_eq!(ffi.max_total_amount, Some(Amount::new(41)));
    assert_eq!(ffi.required_keyset_id.unwrap().hex, "009a1f293253e41e");
}

#[test]
fn omitted_constraints_default_to_unconstrained_send() {
    let mut json = serde_json::to_value(SendOptions::default()).unwrap();
    json.as_object_mut().unwrap().remove("retain_cancellation");
    json.as_object_mut().unwrap().remove("max_total_amount");
    json.as_object_mut().unwrap().remove("required_keyset_id");
    let core: cdk::wallet::SendOptions = decode_send_options(json.to_string())
        .unwrap()
        .try_into()
        .unwrap();
    assert!(!core.retain_cancellation);
    assert!(core.max_total_amount.is_none());
    assert!(core.required_keyset_id.is_none());
}

#[test]
fn invalid_required_keyset_is_rejected_by_conversion() {
    let options = SendOptions {
        required_keyset_id: Some(Id {
            hex: "invalid".to_string(),
        }),
        ..Default::default()
    };
    assert!(cdk::wallet::SendOptions::try_from(options).is_err());
}
