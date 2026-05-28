use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum IdlError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid idl: {0}")]
    Invalid(String),
    #[error("decode error: {0}")]
    Decode(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlDocument {
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
    pub metadata: Option<IdlMetadata>,
    #[serde(default)]
    pub instructions: Vec<IdlInstruction>,
    #[serde(default)]
    pub accounts: Vec<IdlAccount>,
    #[serde(default)]
    pub types: Vec<IdlTypeDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlMetadata {
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub spec: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlInstruction {
    pub name: String,
    #[serde(default)]
    pub discriminator: Option<Vec<u8>>,
    #[serde(default)]
    pub accounts: Vec<IdlAccountItem>,
    #[serde(default)]
    pub args: Vec<IdlField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlAccountItem {
    pub name: String,
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub pda: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlField {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlAccount {
    pub name: String,
    #[serde(default)]
    pub discriminator: Option<Vec<u8>>,
    #[serde(default, rename = "type")]
    pub ty: Option<IdlTypeLayout>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlTypeDefinition {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: IdlTypeLayout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlTypeLayout {
    pub kind: String,
    #[serde(default)]
    pub fields: Vec<IdlFieldEntry>,
    #[serde(default)]
    pub variants: Vec<IdlVariant>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IdlFieldEntry {
    Named(IdlField),
    Unnamed(Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlVariant {
    pub name: String,
    #[serde(default)]
    pub fields: Vec<IdlFieldEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeRef {
    Bool,
    U8,
    U16,
    U32,
    U64,
    U128,
    I64,
    String,
    PublicKey,
    Bytes,
    Vec(Box<TypeRef>),
    Option(Box<TypeRef>),
    Array(Box<TypeRef>, usize),
    Defined(String),
}

impl TypeRef {
    fn parse(value: &Value) -> Result<Self, IdlError> {
        match value {
            Value::String(name) => match name.as_str() {
                "bool" => Ok(Self::Bool),
                "u8" => Ok(Self::U8),
                "u16" => Ok(Self::U16),
                "u32" => Ok(Self::U32),
                "u64" => Ok(Self::U64),
                "u128" => Ok(Self::U128),
                "i64" => Ok(Self::I64),
                "string" => Ok(Self::String),
                "publicKey" | "pubkey" => Ok(Self::PublicKey),
                "bytes" => Ok(Self::Bytes),
                other => Err(IdlError::Invalid(format!(
                    "unsupported primitive type {other}"
                ))),
            },
            Value::Object(map) => {
                if let Some(inner) = map.get("vec") {
                    return Ok(Self::Vec(Box::new(Self::parse(inner)?)));
                }
                if let Some(inner) = map.get("option") {
                    return Ok(Self::Option(Box::new(Self::parse(inner)?)));
                }
                if let Some(inner) = map.get("defined") {
                    return match inner {
                        Value::String(name) => Ok(Self::Defined(name.to_owned())),
                        Value::Object(defined) => defined
                            .get("name")
                            .and_then(Value::as_str)
                            .map(|name| Self::Defined(name.to_owned()))
                            .ok_or_else(|| {
                                IdlError::Invalid("defined type name must be a string".to_owned())
                            }),
                        _ => Err(IdlError::Invalid(
                            "defined type name must be a string".to_owned(),
                        )),
                    };
                }
                if let Some(inner) = map.get("array") {
                    let array = inner.as_array().ok_or_else(|| {
                        IdlError::Invalid("array type must be [inner, len]".to_owned())
                    })?;
                    if array.len() != 2 {
                        return Err(IdlError::Invalid(
                            "array type must have two elements".to_owned(),
                        ));
                    }
                    let element = Self::parse(&array[0])?;
                    let len = array[1]
                        .as_u64()
                        .ok_or_else(|| IdlError::Invalid("array length must be u64".to_owned()))?
                        as usize;
                    return Ok(Self::Array(Box::new(element), len));
                }
                Err(IdlError::Invalid(format!(
                    "unsupported complex type {map:?}"
                )))
            }
            other => Err(IdlError::Invalid(format!(
                "unsupported type descriptor {other:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LoadedIdl {
    pub path: PathBuf,
    pub hash: String,
    pub document: IdlDocument,
    instruction_map: HashMap<[u8; 8], LoadedInstruction>,
    account_map: HashMap<[u8; 8], LoadedAccount>,
    defined_types: HashMap<String, IdlTypeLayout>,
}

#[derive(Debug, Clone)]
struct LoadedInstruction {
    name: String,
    accounts: Vec<String>,
    args: Vec<(String, TypeRef)>,
}

#[derive(Debug, Clone)]
struct LoadedAccount {
    name: String,
    fields: Vec<(String, TypeRef)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecodedInstruction {
    pub name: String,
    pub accounts: Vec<String>,
    pub args: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecodedAccount {
    pub name: String,
    pub fields: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum InstructionDecode {
    Known {
        decoded: DecodedInstruction,
    },
    Unknown {
        discriminator_hex: String,
        data_len: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AccountDecode {
    Known {
        decoded: DecodedAccount,
    },
    Unknown {
        discriminator_hex: String,
        data_len: usize,
    },
}

impl LoadedIdl {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, IdlError> {
        let path = path.as_ref().to_path_buf();
        let raw = fs::read_to_string(&path)?;
        let mut document: IdlDocument = serde_json::from_str(&raw)?;
        if document.name.trim().is_empty() {
            if let Some(metadata_name) = document
                .metadata
                .as_ref()
                .and_then(|meta| meta.name.clone())
            {
                document.name = metadata_name;
            }
        }
        if document.version.is_none() {
            document.version = document
                .metadata
                .as_ref()
                .and_then(|meta| meta.version.clone());
        }
        if document.name.trim().is_empty() {
            return Err(IdlError::Invalid(format!(
                "idl {} is missing a program name",
                path.display()
            )));
        }
        let hash = format!("{:x}", Sha256::digest(raw.as_bytes()));

        let mut defined_types = HashMap::new();
        for defined in &document.types {
            defined_types.insert(defined.name.clone(), defined.ty.clone());
        }

        let mut instruction_map = HashMap::new();
        for instruction in &document.instructions {
            let disc = instruction
                .discriminator
                .clone()
                .map(discriminator_vec_to_array)
                .transpose()?
                .unwrap_or_else(|| anchor_discriminator("global", &instruction.name));
            let args = instruction
                .args
                .iter()
                .map(|field| Ok((field.name.clone(), TypeRef::parse(&field.ty)?)))
                .collect::<Result<Vec<_>, IdlError>>()?;
            instruction_map.insert(
                disc,
                LoadedInstruction {
                    name: instruction.name.clone(),
                    accounts: instruction
                        .accounts
                        .iter()
                        .map(|account| account.name.clone())
                        .collect(),
                    args,
                },
            );
        }

        let mut account_map = HashMap::new();
        for account in &document.accounts {
            let disc = account
                .discriminator
                .clone()
                .map(discriminator_vec_to_array)
                .transpose()?
                .unwrap_or_else(|| anchor_discriminator("account", &account.name));
            let layout = account
                .ty
                .clone()
                .or_else(|| defined_types.get(&account.name).cloned())
                .ok_or_else(|| {
                    IdlError::Invalid(format!(
                        "account {} is missing an inline layout and no matching type definition exists",
                        account.name
                    ))
                })?;
            if layout.kind != "struct" {
                return Err(IdlError::Invalid(format!(
                    "account {} uses unsupported layout kind {}",
                    account.name, layout.kind
                )));
            }
            let fields = layout
                .fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let field = normalized_field(field, index);
                    Ok((field.name.clone(), TypeRef::parse(&field.ty)?))
                })
                .collect::<Result<Vec<_>, IdlError>>()?;
            account_map.insert(
                disc,
                LoadedAccount {
                    name: account.name.clone(),
                    fields,
                },
            );
        }

        Ok(Self {
            path,
            hash,
            document,
            instruction_map,
            account_map,
            defined_types,
        })
    }

    pub fn decode_instruction(&self, data: &[u8]) -> Result<InstructionDecode, IdlError> {
        if data.len() < 8 {
            return Ok(InstructionDecode::Unknown {
                discriminator_hex: hex::encode(data),
                data_len: data.len(),
            });
        }
        let mut discriminator = [0u8; 8];
        discriminator.copy_from_slice(&data[..8]);
        let Some(layout) = self.instruction_map.get(&discriminator) else {
            return Ok(InstructionDecode::Unknown {
                discriminator_hex: hex::encode(discriminator),
                data_len: data.len(),
            });
        };
        let mut cursor = Cursor::new(&data[8..]);
        let mut args = BTreeMap::new();
        for (name, ty) in &layout.args {
            args.insert(name.clone(), self.decode_type(ty, &mut cursor)?);
        }
        cursor.expect_eof()?;
        Ok(InstructionDecode::Known {
            decoded: DecodedInstruction {
                name: layout.name.clone(),
                accounts: layout.accounts.clone(),
                args,
            },
        })
    }

    pub fn program_address(&self) -> Option<&str> {
        self.document
            .address
            .as_deref()
            .or_else(|| self.document.metadata.as_ref()?.address.as_deref())
    }

    pub fn instruction_names(&self) -> Vec<String> {
        let mut names = self
            .document
            .instructions
            .iter()
            .map(|instruction| instruction.name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    pub fn account_names(&self) -> Vec<String> {
        let mut names = self
            .document
            .accounts
            .iter()
            .map(|account| account.name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    pub fn instruction_account_names(&self, name: &str) -> Option<Vec<String>> {
        self.document
            .instructions
            .iter()
            .find(|instruction| instruction.name == name)
            .map(|instruction| {
                instruction
                    .accounts
                    .iter()
                    .map(|account| account.name.clone())
                    .collect()
            })
    }

    pub fn instruction_discriminator_hex(&self, name: &str) -> Option<String> {
        self.document
            .instructions
            .iter()
            .find(|instruction| instruction.name == name)
            .and_then(|instruction| {
                instruction
                    .discriminator
                    .clone()
                    .map(discriminator_vec_to_array)
                    .transpose()
                    .ok()
                    .flatten()
            })
            .or_else(|| {
                self.document
                    .instructions
                    .iter()
                    .any(|instruction| instruction.name == name)
                    .then(|| anchor_discriminator("global", name))
            })
            .map(hex::encode)
    }

    pub fn account_field_names(&self, name: &str) -> Option<Vec<String>> {
        let layout = self
            .document
            .accounts
            .iter()
            .find(|account| account.name == name)
            .and_then(|account| account.ty.clone())
            .or_else(|| self.defined_types.get(name).cloned())?;
        Some(
            layout
                .fields
                .iter()
                .enumerate()
                .map(|(index, field)| normalized_field(field, index).name.clone())
                .collect(),
        )
    }

    pub fn decode_account(&self, data: &[u8]) -> Result<AccountDecode, IdlError> {
        if data.len() < 8 {
            return Ok(AccountDecode::Unknown {
                discriminator_hex: hex::encode(data),
                data_len: data.len(),
            });
        }
        let mut discriminator = [0u8; 8];
        discriminator.copy_from_slice(&data[..8]);
        let Some(layout) = self.account_map.get(&discriminator) else {
            return Ok(AccountDecode::Unknown {
                discriminator_hex: hex::encode(discriminator),
                data_len: data.len(),
            });
        };
        let mut cursor = Cursor::new(&data[8..]);
        let mut fields = BTreeMap::new();
        for (name, ty) in &layout.fields {
            fields.insert(name.clone(), self.decode_type(ty, &mut cursor)?);
        }
        cursor.expect_eof()?;
        Ok(AccountDecode::Known {
            decoded: DecodedAccount {
                name: layout.name.clone(),
                fields,
            },
        })
    }

    fn decode_type(&self, ty: &TypeRef, cursor: &mut Cursor<'_>) -> Result<Value, IdlError> {
        match ty {
            TypeRef::Bool => Ok(Value::Bool(cursor.read_u8()? != 0)),
            TypeRef::U8 => Ok(Value::from(cursor.read_u8()?)),
            TypeRef::U16 => Ok(Value::from(cursor.read_u16()?)),
            TypeRef::U32 => Ok(Value::from(cursor.read_u32()?)),
            TypeRef::U64 => Ok(Value::from(cursor.read_u64()?)),
            TypeRef::U128 => Ok(Value::String(cursor.read_u128()?.to_string())),
            TypeRef::I64 => Ok(Value::from(cursor.read_i64()?)),
            TypeRef::String => Ok(Value::String(cursor.read_string()?)),
            TypeRef::PublicKey => {
                let bytes = cursor.read_fixed(32)?;
                Ok(Value::String(bs58::encode(bytes).into_string()))
            }
            TypeRef::Bytes => Ok(Value::String(hex::encode(cursor.read_vec()?))),
            TypeRef::Vec(inner) => {
                let len = cursor.read_u32()? as usize;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    out.push(self.decode_type(inner, cursor)?);
                }
                Ok(Value::Array(out))
            }
            TypeRef::Option(inner) => {
                let tag = cursor.read_u8()?;
                if tag == 0 {
                    Ok(Value::Null)
                } else {
                    self.decode_type(inner, cursor)
                }
            }
            TypeRef::Array(inner, len) => {
                let mut out = Vec::with_capacity(*len);
                for _ in 0..*len {
                    out.push(self.decode_type(inner, cursor)?);
                }
                Ok(Value::Array(out))
            }
            TypeRef::Defined(name) => {
                let layout = self
                    .defined_types
                    .get(name)
                    .ok_or_else(|| IdlError::Invalid(format!("unknown defined type {name}")))?;
                match layout.kind.as_str() {
                    "struct" => {
                        let mut map = Map::new();
                        for (index, field) in layout.fields.iter().enumerate() {
                            let field = normalized_field(field, index);
                            let field_ty = TypeRef::parse(&field.ty)?;
                            map.insert(field.name.clone(), self.decode_type(&field_ty, cursor)?);
                        }
                        Ok(Value::Object(map))
                    }
                    "enum" => {
                        let variant_index = cursor.read_u8()? as usize;
                        let variant = layout.variants.get(variant_index).ok_or_else(|| {
                            IdlError::Decode(format!(
                                "enum variant index {} out of bounds for {}",
                                variant_index, name
                            ))
                        })?;
                        if variant.fields.is_empty() {
                            return Ok(Value::String(variant.name.clone()));
                        }
                        let mut fields = Vec::with_capacity(variant.fields.len());
                        for (index, field) in variant.fields.iter().enumerate() {
                            let field = normalized_field(field, index);
                            let field_ty = TypeRef::parse(&field.ty)?;
                            fields.push(self.decode_type(&field_ty, cursor)?);
                        }
                        Ok(serde_json::json!({
                            "variant": variant.name,
                            "fields": fields,
                        }))
                    }
                    other => Err(IdlError::Invalid(format!(
                        "defined type {name} uses unsupported kind {other}"
                    ))),
                }
            }
        }
    }
}

fn normalized_field(field: &IdlFieldEntry, index: usize) -> IdlField {
    match field {
        IdlFieldEntry::Named(field) => field.clone(),
        IdlFieldEntry::Unnamed(ty) => IdlField {
            name: format!("field{index}"),
            ty: ty.clone(),
        },
    }
}

fn discriminator_vec_to_array(value: Vec<u8>) -> Result<[u8; 8], IdlError> {
    if value.len() != 8 {
        return Err(IdlError::Invalid(format!(
            "discriminator must be 8 bytes, got {}",
            value.len()
        )));
    }
    let mut out = [0u8; 8];
    out.copy_from_slice(&value);
    Ok(out)
}

pub fn anchor_discriminator(namespace: &str, name: &str) -> [u8; 8] {
    let mut hasher = Sha256::new();
    hasher.update(format!("{namespace}:{name}").as_bytes());
    let hash = hasher.finalize();
    let mut discriminator = [0u8; 8];
    discriminator.copy_from_slice(&hash[..8]);
    discriminator
}

struct Cursor<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    fn read_fixed(&mut self, len: usize) -> Result<&'a [u8], IdlError> {
        let end = self.offset.saturating_add(len);
        if end > self.data.len() {
            return Err(IdlError::Decode("buffer underflow".to_owned()));
        }
        let slice = &self.data[self.offset..end];
        self.offset = end;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u8, IdlError> {
        Ok(self.read_fixed(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, IdlError> {
        let mut bytes = [0u8; 2];
        bytes.copy_from_slice(self.read_fixed(2)?);
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32, IdlError> {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(self.read_fixed(4)?);
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, IdlError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.read_fixed(8)?);
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_u128(&mut self) -> Result<u128, IdlError> {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(self.read_fixed(16)?);
        Ok(u128::from_le_bytes(bytes))
    }

    fn read_i64(&mut self) -> Result<i64, IdlError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.read_fixed(8)?);
        Ok(i64::from_le_bytes(bytes))
    }

    fn read_string(&mut self) -> Result<String, IdlError> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_fixed(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|error| IdlError::Decode(format!("invalid utf8 string: {error}")))
    }

    fn read_vec(&mut self) -> Result<Vec<u8>, IdlError> {
        let len = self.read_u32()? as usize;
        Ok(self.read_fixed(len)?.to_vec())
    }

    fn expect_eof(&self) -> Result<(), IdlError> {
        if self.offset == self.data.len() {
            Ok(())
        } else {
            Err(IdlError::Decode(format!(
                "expected eof, {} trailing bytes remain",
                self.data.len() - self.offset
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use serde_json::Value;

    use super::{AccountDecode, IdlError, InstructionDecode, LoadedIdl, anchor_discriminator};

    fn fixture_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("fixtures")
            .join("idl")
            .join("pump_mock_idl.json")
    }

    fn official_pumpfun_idl_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("vendor")
            .join("pumpfun")
            .join("idl")
            .join("pump.json")
    }

    fn encode_string(value: &str) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(value.as_bytes());
        bytes
    }

    fn encode_pubkey(value: &str) -> Vec<u8> {
        bs58::decode(value).into_vec().expect("pubkey")
    }

    #[test]
    fn loads_idl_and_hashes_it() {
        let loaded = LoadedIdl::load(fixture_path()).expect("idl");
        assert_eq!(loaded.document.name, "pump");
        assert_eq!(loaded.hash.len(), 64);
    }

    #[test]
    fn loads_official_pumpfun_idl_and_exposes_contract_fields() {
        let loaded = LoadedIdl::load(official_pumpfun_idl_path()).expect("official idl");
        assert_eq!(
            loaded.program_address(),
            Some("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
        );
        let instructions = loaded.instruction_names();
        for expected in [
            "create",
            "create_v2",
            "buy",
            "sell",
            "buy_v2",
            "sell_v2",
            "buy_exact_quote_in_v2",
            "buy_exact_sol_in",
        ] {
            assert!(
                instructions.iter().any(|name| name == expected),
                "missing {expected}"
            );
        }
        let buy_accounts = loaded
            .instruction_account_names("buy")
            .expect("buy accounts");
        assert!(
            buy_accounts
                .iter()
                .any(|name| name == "associated_bonding_curve")
        );
        let buy_v2_accounts = loaded
            .instruction_account_names("buy_v2")
            .expect("buy_v2 accounts");
        assert!(
            buy_v2_accounts
                .iter()
                .any(|name| name == "associated_base_bonding_curve")
        );
        assert!(
            buy_v2_accounts
                .iter()
                .any(|name| name == "associated_quote_bonding_curve")
        );
        let fields = loaded
            .account_field_names("BondingCurve")
            .expect("bonding curve fields");
        for expected in [
            "virtual_token_reserves",
            "virtual_quote_reserves",
            "real_token_reserves",
            "real_quote_reserves",
            "token_total_supply",
            "complete",
            "creator",
            "quote_mint",
        ] {
            assert!(
                fields.iter().any(|field| field == expected),
                "missing field {expected}"
            );
        }
    }

    #[test]
    fn loads_modern_anchor_style_idl() {
        let temp_path = std::env::temp_dir().join(format!(
            "pump-launch-quant-modern-idl-{}.json",
            std::process::id()
        ));
        let raw = r#"{
          "address": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
          "metadata": {
            "name": "pump",
            "version": "0.1.0",
            "spec": "0.1.0",
            "description": "Created with Anchor"
          },
          "instructions": [
            {
              "name": "buy",
              "discriminator": [102, 6, 61, 18, 1, 218, 235, 234],
              "accounts": [
                {"name": "mint"},
                {"name": "user"}
              ],
              "args": [
                {"name": "amount", "type": "u64"},
                {"name": "track_volume", "type": {"defined": {"name": "OptionBool"}}}
              ]
            }
          ],
          "accounts": [
            {
              "name": "BondingCurve",
              "discriminator": [23, 183, 248, 55, 96, 216, 172, 96]
            }
          ],
          "types": [
            {
              "name": "BondingCurve",
              "type": {
                "kind": "struct",
                "fields": [
                  {"name": "virtual_quote_reserves", "type": "u64"},
                  {"name": "quote_mint", "type": "pubkey"},
                  {"name": "fee_tier", "type": {"defined": {"name": "FeeTier"}}}
                ]
              }
            },
            {
              "name": "FeeTier",
              "type": {
                "kind": "struct",
                "fields": [
                  {"name": "market_cap_lamports_threshold", "type": "u128"}
                ]
              }
            },
            {
              "name": "OptionBool",
              "type": {
                "kind": "struct",
                "fields": ["bool"]
              }
            }
          ]
        }"#;
        fs::write(&temp_path, raw).expect("write modern idl");
        let loaded = LoadedIdl::load(&temp_path).expect("load modern idl");
        assert_eq!(loaded.document.name, "pump");
        assert_eq!(loaded.document.version.as_deref(), Some("0.1.0"));
        fs::remove_file(temp_path).ok();
    }

    #[test]
    fn decodes_create_instruction() {
        let loaded = LoadedIdl::load(fixture_path()).expect("idl");
        let mut data = Vec::new();
        data.extend_from_slice(&anchor_discriminator("global", "create"));
        data.extend_from_slice(&encode_string("alpha"));
        data.extend_from_slice(&encode_string("ALP"));
        data.extend_from_slice(&encode_string("https://example.invalid/alpha.json"));
        data.extend_from_slice(&encode_pubkey(
            "So11111111111111111111111111111111111111112",
        ));
        let decoded = loaded.decode_instruction(&data).expect("decoded");
        let InstructionDecode::Known { decoded } = decoded else {
            panic!("expected known decode");
        };
        assert_eq!(decoded.name, "create");
        assert_eq!(decoded.args["name"], Value::String("alpha".to_owned()));
    }

    #[test]
    fn decodes_buy_instruction() {
        let loaded = LoadedIdl::load(fixture_path()).expect("idl");
        let mut data = Vec::new();
        data.extend_from_slice(&anchor_discriminator("global", "buy"));
        data.extend_from_slice(&123u64.to_le_bytes());
        data.extend_from_slice(&456u64.to_le_bytes());
        let decoded = loaded.decode_instruction(&data).expect("decoded");
        let InstructionDecode::Known { decoded } = decoded else {
            panic!("expected known decode");
        };
        assert_eq!(decoded.args["quote_in"], Value::from(123u64));
        assert_eq!(decoded.args["min_token_out"], Value::from(456u64));
    }

    #[test]
    fn decodes_bonding_curve_account() {
        let loaded = LoadedIdl::load(fixture_path()).expect("idl");
        let mut data = Vec::new();
        data.extend_from_slice(&anchor_discriminator("account", "BondingCurve"));
        data.extend_from_slice(&10u64.to_le_bytes());
        data.extend_from_slice(&20u64.to_le_bytes());
        data.extend_from_slice(&30u64.to_le_bytes());
        data.extend_from_slice(&40u64.to_le_bytes());
        data.extend_from_slice(&encode_pubkey(
            "So11111111111111111111111111111111111111112",
        ));
        data.push(1);
        let decoded = loaded.decode_account(&data).expect("decoded");
        let AccountDecode::Known { decoded } = decoded else {
            panic!("expected known decode");
        };
        assert_eq!(decoded.name, "BondingCurve");
        assert_eq!(decoded.fields["complete"], Value::Bool(true));
    }

    #[test]
    fn returns_unknown_for_unmapped_instruction() {
        let loaded = LoadedIdl::load(fixture_path()).expect("idl");
        let decoded = loaded
            .decode_instruction(&[1, 2, 3, 4, 5, 6, 7, 8, 9])
            .expect("decode");
        assert!(matches!(decoded, InstructionDecode::Unknown { .. }));
    }

    #[test]
    fn rejects_invalid_discriminator_length() {
        let broken = r#"{
          "name": "broken",
          "instructions": [{"name":"bad","discriminator":[1,2],"accounts":[],"args":[]}],
          "accounts": [],
          "types": []
        }"#;
        let file = tempfile::NamedTempFile::new().expect("tempfile");
        std::fs::write(file.path(), broken).expect("write");
        let error = LoadedIdl::load(file.path()).expect_err("must fail");
        assert!(matches!(error, IdlError::Invalid(_)));
    }
}
