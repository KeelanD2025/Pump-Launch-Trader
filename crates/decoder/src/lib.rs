use common::config::LoadedConfig;
use idl::{DecodedInstruction, InstructionDecode, LoadedIdl};

#[derive(Debug, thiserror::Error)]
pub enum DecoderError {
    #[error("idl error: {0}")]
    Idl(#[from] idl::IdlError),
    #[error("no idls configured")]
    NoIdlsConfigured,
}

#[derive(Debug, Clone)]
pub struct DecoderRegistry {
    idls: Vec<LoadedIdl>,
}

impl DecoderRegistry {
    pub fn from_config(config: &LoadedConfig) -> Result<Self, DecoderError> {
        if config.config.pump.idl_paths.is_empty() {
            return Err(DecoderError::NoIdlsConfigured);
        }
        let mut idls = Vec::new();
        for path in &config.config.pump.idl_paths {
            idls.push(LoadedIdl::load(config.resolve_path(path))?);
        }
        Ok(Self { idls })
    }

    pub fn decode_instruction(
        &self,
        data: &[u8],
    ) -> Result<Option<DecodedInstruction>, DecoderError> {
        for idl in &self.idls {
            match idl.decode_instruction(data)? {
                InstructionDecode::Known { decoded } => return Ok(Some(decoded)),
                InstructionDecode::Unknown { .. } => continue,
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use common::config::LoadedConfig;
    use idl::anchor_discriminator;

    use super::DecoderRegistry;

    fn config() -> LoadedConfig {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("config")
            .join("default.toml");
        LoadedConfig::from_file(path).expect("config")
    }

    #[test]
    fn resolves_instruction_using_loaded_idl_set() {
        let config = config();
        let registry = DecoderRegistry::from_config(&config).expect("registry");
        let mut data = Vec::new();
        data.extend_from_slice(&anchor_discriminator("global", "buy"));
        data.extend_from_slice(&10u64.to_le_bytes());
        data.extend_from_slice(&20u64.to_le_bytes());
        let decoded = registry
            .decode_instruction(&data)
            .expect("decode")
            .expect("known decode");
        assert_eq!(decoded.name, "buy");
    }
}
