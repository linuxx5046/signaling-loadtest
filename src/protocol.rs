//! Formas de mensaje del wire protocol, definidas de forma independiente
//! del servidor (este crate simula clientes reales desde afuera -- no
//! debe depender de los tipos internos de `signaling-rust`). Mismos
//! literales que `especificacion-compatibilidad-v1.md` §3.

use serde_json::{json, Value};

/// Construye un mensaje entrante válido (cliente -> servidor).
/// `encrypted_payload` es opaco por diseño -- nunca se inspecciona, así
/// que un string cualquiera de longitud realista sirve.
pub fn make_message(request_id: &str, method: &str, target_client_id: &str) -> Value {
    json!({
        "requestId": request_id,
        "method": method,
        "encryptedPayload": fake_payload(),
        "targetClientId": target_client_id,
    })
}

fn fake_payload() -> String {
    // 64 bytes en hex -- del tamaño de un payload cifrado real típico,
    // sin pretender serlo (nunca se inspecciona en ningún punto del
    // protocolo, sección 3.1/4 de la spec).
    (0..64)
        .map(|_| format!("{:02x}", rand::random::<u8>()))
        .collect()
}

/// Lee el campo `info` de una respuesta del servidor, si existe.
pub fn info_of(value: &Value) -> Option<&str> {
    value.get("info").and_then(|v| v.as_str())
}
